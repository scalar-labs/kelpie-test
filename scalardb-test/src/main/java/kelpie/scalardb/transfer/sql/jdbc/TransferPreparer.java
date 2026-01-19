package kelpie.scalardb.transfer.sql.jdbc;

import com.scalar.db.sql.TransactionMode;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PreProcessor;
import com.zaxxer.hikari.HikariDataSource;
import io.github.resilience4j.retry.Retry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import kelpie.scalardb.Common;
import kelpie.scalardb.transfer.TransferCommon;
import kelpie.scalardb.transfer.sql.SqlCommon;

public class TransferPreparer extends PreProcessor {
  private static final long DEFAULT_POPULATION_CONCURRENCY = 32L;
  private static final long DEFAULT_POPULATION_NUM_ACCOUNTS_PER_TX = 100;
  private static final long DEFAULT_POPULATION_MAX_RETRIES = 10;
  private static final long DEFAULT_POPULATION_WAIT_MILLS = 1000;

  private final HikariDataSource dataSource;

  public TransferPreparer(Config config) {
    super(config);
    dataSource = SqlCommon.getDataSource(config, TransactionMode.TRANSACTION);
  }

  @Override
  public void execute() {
    logInfo("insert initial values... ");

    int concurrency =
        (int)
            config.getUserLong(
                "test_config", "population_concurrency", DEFAULT_POPULATION_CONCURRENCY);
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
  public void close() {
    dataSource.close();
  }

  private class PopulationRunner {
    private final int id;

    public PopulationRunner(int threadId) {
      this.id = threadId;
    }

    public void run() {
      int concurrency =
          (int)
              config.getUserLong(
                  "test_config", "population_concurrency", DEFAULT_POPULATION_CONCURRENCY);
      int numAccountsPerTx =
          (int)
              config.getUserLong(
                  "test_config",
                  "population_num_accounts_per_tx",
                  DEFAULT_POPULATION_NUM_ACCOUNTS_PER_TX);
      int numAccounts = (int) config.getUserLong("test_config", "num_accounts");
      int numPerThread = (numAccounts + concurrency - 1) / concurrency;
      int start = numPerThread * id;
      int end = Math.min(numPerThread * (id + 1), numAccounts);
      IntStream.range(0, (numPerThread + numAccountsPerTx - 1) / numAccountsPerTx)
          .forEach(
              i -> {
                int startId = start + numAccountsPerTx * i;
                int endId = Math.min(start + numAccountsPerTx * (i + 1), end);
                populateWithTx(startId, endId);
              });
    }

    private void populateWithTx(int startId, int endId) {
      Runnable populate =
          () -> {
            try (Connection connection = dataSource.getConnection()) {
              try (PreparedStatement preparedStatement =
                  connection.prepareStatement(
                      "INSERT INTO "
                          + TransferCommon.NAMESPACE
                          + "."
                          + TransferCommon.TABLE
                          + " VALUES(?,?,?)")) {

                for (int i = startId; i < endId; ++i) {
                  for (int j = 0; j < TransferCommon.NUM_TYPES; ++j) {
                    preparedStatement.clearParameters();
                    preparedStatement.setInt(1, i);
                    preparedStatement.setInt(2, j);
                    preparedStatement.setInt(3, TransferCommon.INITIAL_BALANCE);
                    preparedStatement.execute();
                  }
                }

                connection.commit();
              } catch (SQLException e) {
                connection.rollback();
                throw e;
              }
            } catch (SQLException e) {
              String message = "population failed.";
              logWarn(message, e);
              throw new RuntimeException(message, e);
            }
          };

      int maxRetries =
          (int)
              config.getUserLong(
                  "test_config", "population_max_retries", DEFAULT_POPULATION_MAX_RETRIES);
      int waitMillis =
          (int)
              config.getUserLong(
                  "test_config", "population_wait_millis", DEFAULT_POPULATION_WAIT_MILLS);
      Retry retry = Common.getRetryWithFixedWaitDuration("populate", maxRetries, waitMillis);
      Runnable decorated = Retry.decorateRunnable(retry, populate);
      try {
        decorated.run();
      } catch (Exception e) {
        logError("population failed repeatedly!");
        throw e;
      }
    }
  }
}
