package kelpie.scalardb.ycsb.sql;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import javax.json.Json;

/** Workload C: Read only. */
public class WorkloadC extends TimeBasedProcessor {
  private static final long DEFAULT_OPS_PER_TX = 2; // two read operations

  private final ThreadLocal<String> jdbcUrl;

  private final int tableCount;
  private final long recordCount;
  private final int opsPerTx;

  private final LongAdder transactionRetryCount = new LongAdder();

  public WorkloadC(Config config) {
    super(config);

    jdbcUrl =
        ThreadLocal.withInitial(
            () ->
                "jdbc:scalardb:"
                    + YcsbCommon.getConfigFile(config)
                    + "?dummy="
                    + ThreadLocalRandom.current().nextLong());

    tableCount = YcsbCommon.getTableCount(config);
    recordCount = YcsbCommon.getRecordCount(config);
    opsPerTx =
        (int) config.getUserLong(YcsbCommon.CONFIG_NAME, YcsbCommon.OPS_PER_TX, DEFAULT_OPS_PER_TX);
  }

  @Override
  public void executeEach() throws Exception {
    List<Long> userIds = new ArrayList<>(opsPerTx);
    for (int i = 0; i < opsPerTx; ++i) {
      userIds.add(ThreadLocalRandom.current().nextLong(recordCount));
    }

    while (true) {
      try (Connection connection = DriverManager.getConnection(jdbcUrl.get())) {
        for (Long userId : userIds) {
          try (PreparedStatement preparedStatement =
              connection.prepareStatement(
                  "SELECT * FROM "
                      + YcsbCommon.getTableName(userId, tableCount)
                      + " WHERE "
                      + YcsbCommon.YCSB_KEY
                      + " = ?")) {
            preparedStatement.setLong(1, userId);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
              boolean hasNext = resultSet.next();
              if (!hasNext) {
                logWarn("The record with the key " + userId + " does not exist");
              }
            }
          }
        }

        break;
      } catch (SQLTransientException e) {
        transactionRetryCount.increment();
      }
    }
  }

  @Override
  public void close() {
    setState(
        Json.createObjectBuilder()
            .add("transaction-retry-count", transactionRetryCount.toString())
            .build());
  }
}
