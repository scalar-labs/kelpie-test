package kelpie.jdbc.ycsb;

import static kelpie.jdbc.ycsb.YcsbCommon.CONFIG_NAME;
import static kelpie.jdbc.ycsb.YcsbCommon.DB_CONFIG_NAME;
import static kelpie.jdbc.ycsb.YcsbCommon.OPS_PER_TX;
import static kelpie.jdbc.ycsb.YcsbCommon.getRecordCount;
import static kelpie.jdbc.ycsb.YcsbCommon.read;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import javax.json.Json;
import kelpie.jdbc.Common;
import kelpie.jdbc.DataSourceManager;

public class WorkloadC extends TimeBasedProcessor {
  private static final long DEFAULT_OPS_PER_TX = 2;
  private final DataSourceManager manager;
  private final int recordCount;
  private final int opsPerTx;

  private final LongAdder transactionRetryCount = new LongAdder();

  public WorkloadC(Config config) {
    super(config);
    this.recordCount = getRecordCount(config);
    this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);

    manager = new DataSourceManager(config, DB_CONFIG_NAME);
  }

  @Override
  public void executeEach() throws SQLException {
    List<Integer> userIds = new ArrayList<>(opsPerTx);
    for (int i = 0; i < opsPerTx; ++i) {
      userIds.add(ThreadLocalRandom.current().nextInt(recordCount));
    }

    Connection connection = null;
    while (true) {
      connection = manager.getConnection();
      connection.setAutoCommit(false);
      try {
        for (int i = 0; i < userIds.size(); i++) {
          int userId = userIds.get(i);
          read(connection, userId);
        }
        connection.commit();
        break;
      } catch (SQLException e) {
        connection.rollback();
        e.printStackTrace();
        transactionRetryCount.increment();
      } catch (Exception e) {
        connection.rollback();
        throw e;
      } finally {
        if (connection != null) {
          connection.close();
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    setState(
        Json.createObjectBuilder()
            .add("transaction-retry-count", transactionRetryCount.toString())
            .build());
  }
}
