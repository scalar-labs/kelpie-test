package kelpie.seata.mycsb;

import static kelpie.seata.mycsb.MycsbCommon.CONFIG_NAME;
import static kelpie.seata.mycsb.MycsbCommon.PRIMARY_DB_CONFIG_NAME;
import static kelpie.seata.mycsb.MycsbCommon.SECONDARY_DB_CONFIG_NAME;
import static kelpie.seata.mycsb.MycsbCommon.TABLE;
import static kelpie.seata.mycsb.MycsbCommon.YCSB_KEY;
import static kelpie.seata.mycsb.MycsbCommon.PAYLOAD;
import static kelpie.seata.mycsb.MycsbCommon.getPayloadSize;
import static kelpie.seata.mycsb.MycsbCommon.getRecordCount;
import static kelpie.seata.mycsb.MycsbCommon.randomFastChars;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PreProcessor;
import io.github.resilience4j.retry.Retry;
import io.seata.rm.RMClient;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import kelpie.seata.Common;
import kelpie.seata.DataSourceManager;

public class Loader extends PreProcessor {
  private static final long DEFAULT_POPULATION_CONCURRENCY = 10L;
  private static final long DEFAULT_BATCH_SIZE = 1;
  private static final long DEFAULT_INSERT_BATCH_SIZE = 100;
  private static final String POPULATION_CONCURRENCY = "population_concurrency";
  private static final String BATCH_SIZE = "batch_size";
  private static final String INSERT_BATCH_SIZE = "population_insert_batch_size";
  private static final int MAX_RETRIES = 10;
  private static final int WAIT_DURATION_MILLIS = 1000;
  private static final String INSERT_SQL = "insert into " + TABLE + "(" + YCSB_KEY + ", " + PAYLOAD +") values (?, ?)";
  private final DataSourceManager primary;
  private final DataSourceManager secondary;
  private final int concurrency;
  private final int recordCount;
  private final int payloadSize;
  private final char[] payload;
  private final int batchSize;
  private final int insertBatchSize;

  public Loader(Config config) throws SQLException {
    super(config);
    concurrency =
        (int)
            config.getUserLong(CONFIG_NAME, POPULATION_CONCURRENCY, DEFAULT_POPULATION_CONCURRENCY);
    batchSize = (int) config.getUserLong(CONFIG_NAME, BATCH_SIZE, DEFAULT_BATCH_SIZE);
    insertBatchSize = (int) config.getUserLong(CONFIG_NAME, INSERT_BATCH_SIZE, DEFAULT_INSERT_BATCH_SIZE);
    recordCount = getRecordCount(config);
    payloadSize = getPayloadSize(config);
    payload = new char[payloadSize];

    RMClient.init(Common.APPLICATION_ID, Common.TRANSACTION_SERVICE_GROUP);
    primary = new DataSourceManager(config, PRIMARY_DB_CONFIG_NAME);
    secondary = new DataSourceManager(config, SECONDARY_DB_CONFIG_NAME);
    createTable(primary, payloadSize);
    createTable(secondary, payloadSize);
  }

  @Override
  public void execute() {
    ExecutorService es = Executors.newCachedThreadPool();
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    IntStream.range(0, concurrency)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(() -> new PopulationRunner(i).run(), es);
              futures.add(future);
            });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    logInfo("all records have been inserted");
  }

  @Override
  public void close() throws Exception {
  }

  private class PopulationRunner {
    private final int id;

    public PopulationRunner(int threadId) {
      this.id = threadId;
    }

    public void run() {
      int numPerThread = (recordCount + concurrency - 1) / concurrency;
      int start = numPerThread * id;
      int end = Math.min(numPerThread * (id + 1), recordCount);
      IntStream.range(0, (numPerThread + batchSize - 1) / batchSize)
          .forEach(
              i -> {
                int startId = start + batchSize * i;
                int endId = Math.min(start + batchSize * (i + 1), end);
                populateWithTx(startId, endId);
              });
    }

    private void populateWithTx(int startId, int endId) {
      Runnable populate =
          () -> {
            Connection primaryConnection = null;
            Connection secondaryConnection = null;
            PreparedStatement primaryStatement = null;
            PreparedStatement secondaryStatement = null;
            try {
              primaryConnection = primary.getConnectionXA();
              primaryStatement = primaryConnection.prepareStatement(INSERT_SQL);
              secondaryConnection = secondary.getConnectionXA();               
              secondaryStatement = secondaryConnection.prepareStatement(INSERT_SQL);
              for (int i = startId; i < endId; ++i) {
                randomFastChars(ThreadLocalRandom.current(), payload);
                prepareInsert(primaryStatement, i, new String(payload));
                prepareInsert(secondaryStatement, i, new String(payload));
                if (i % insertBatchSize == 0 || i == endId - 1) {
                  primaryStatement.executeBatch();
                  secondaryStatement.executeBatch();
                }
              }
            } catch (SQLException e) {
              logWarn("population failed.", e);
              throw new RuntimeException("population failed.", e);
            } finally {
              try {
                if (primaryConnection != null) {
                  primaryConnection.close();
                }
                if (secondaryConnection != null) {
                  secondaryConnection.close();
                }
                if (primaryStatement != null) {
                  primaryStatement.close();
                }
                if (secondaryStatement != null) {
                  secondaryStatement.close();
                }
              } catch (SQLException e) {
                logWarn("population failed.", e);
                throw new RuntimeException("population failed.", e);
              }
            }
          };

      Retry retry =
          Common.getRetryWithFixedWaitDuration("populate", MAX_RETRIES, WAIT_DURATION_MILLIS);
      Runnable decorated = Retry.decorateRunnable(retry, populate);
      try {
        decorated.run();
      } catch (Exception e) {
        logError("population failed repeatedly!");
        throw e;
      }
    }
 
    private void prepareInsert(PreparedStatement statement, int id, String payload) throws SQLException {
      try {
        statement.setInt(1, id);
        statement.setString(2, payload);
        statement.addBatch();
      } catch (SQLException e) {
        statement.close();
        throw e;
      }
    }
  }

  private void createTable(DataSourceManager ds, int payloadSize) throws SQLException {
    String dropSQL = "drop table if exists " + TABLE;
    String createSQL = "create table " + TABLE
        + " (ycsb_key int not null, payload varchar(" + payloadSize + ")," + "primary key (ycsb_key))";
    Connection connection = null;
    Statement statement = null;
    try {
      connection = ds.getConnectionXA();
      statement = connection.createStatement();
      statement.executeUpdate(dropSQL);
      statement.executeUpdate(createSQL);
    } catch (SQLException e) {
      throw e;
    } finally {
      if (statement != null) {
        statement.close();
      }
      if (connection != null) {
        connection.close();
      }
    }
  }
}
