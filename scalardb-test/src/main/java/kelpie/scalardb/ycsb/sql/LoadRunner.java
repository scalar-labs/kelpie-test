package kelpie.scalardb.ycsb.sql;

import com.scalar.kelpie.config.Config;
import io.github.resilience4j.retry.Retry;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;
import kelpie.scalardb.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadRunner.class);

  private final String jdbcUrl;
  private final int id;
  private final int concurrency;
  private final int tableCount;
  private final long recordCount;
  private final char[] payload;
  private final long batchSize;
  private final boolean overwrite;

  public LoadRunner(Config config, String jdbcUrl, int threadId) {
    this.id = threadId;
    this.jdbcUrl = jdbcUrl;
    concurrency = YcsbCommon.getLoadConcurrency(config);
    batchSize = YcsbCommon.getLoadBatchSize(config);
    tableCount = YcsbCommon.getTableCount(config);
    recordCount = YcsbCommon.getRecordCount(config);
    payload = new char[YcsbCommon.getPayloadSize(config)];
    overwrite = YcsbCommon.getLoadOverwrite(config);
  }

  public void run() {
    long numPerThread = (recordCount + concurrency - 1) / concurrency;
    long start = numPerThread * id;
    long end = Math.min(numPerThread * (id + 1), recordCount);
    LongStream.range(0, (numPerThread + batchSize - 1) / batchSize)
        .forEach(
            i -> {
              long startId = start + batchSize * i;
              long endId = Math.min(start + batchSize * (i + 1), end);
              populateWithTx(startId, endId);
            });
  }

  private void populateWithTx(long startId, long endId) {
    Runnable populate =
        () -> {
          try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
            for (long i = startId; i < endId; ++i) {
              YcsbCommon.randomFastChars(ThreadLocalRandom.current(), payload);
              put(connection, i, new String(payload));
            }
          } catch (Exception e) {
            LOGGER.warn("Load failed", e);
            throw new RuntimeException("Load failed", e);
          }
        };

    Retry retry = Common.getRetryWithFixedWaitDuration("load");
    Runnable decorated = Retry.decorateRunnable(retry, populate);
    try {
      decorated.run();
    } catch (Exception e) {
      LOGGER.error("Load failed repeatedly!");
      throw e;
    }
  }

  private void put(Connection connection, long userId, String payload) throws SQLException {
    String sql;
    if (overwrite) {
      sql = "UPSERT INTO " + YcsbCommon.getTableName(userId, tableCount) + " VALUES (?,?)";
    } else {
      sql = "INSERT INTO " + YcsbCommon.getTableName(userId, tableCount) + " VALUES (?,?)";
    }

    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      preparedStatement.setLong(1, userId);
      preparedStatement.setString(2, payload);
      preparedStatement.executeUpdate();
    }
  }
}
