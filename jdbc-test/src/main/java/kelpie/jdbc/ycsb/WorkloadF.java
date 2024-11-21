package kelpie.jdbc.ycsb;

import static kelpie.jdbc.ycsb.YcsbCommon.CONFIG_NAME;
import static kelpie.jdbc.ycsb.YcsbCommon.DB_CONFIG_NAME;
import static kelpie.jdbc.ycsb.YcsbCommon.OPS_PER_TX;
import static kelpie.jdbc.ycsb.YcsbCommon.getPayloadSize;
import static kelpie.jdbc.ycsb.YcsbCommon.getRecordCount;
import static kelpie.jdbc.ycsb.YcsbCommon.read;
import static kelpie.jdbc.ycsb.YcsbCommon.write;

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

public class WorkloadF extends TimeBasedProcessor {
  private static final long DEFAULT_OPS_PER_TX = 1;
  private final DataSourceManager manager;
  private final int recordCount;
  private final int opsPerTx;
  private final int payloadSize;

  private final LongAdder transactionRetryCount = new LongAdder();

  public WorkloadF(Config config) {
    super(config);
    this.recordCount = getRecordCount(config);
    this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);
    this.payloadSize = getPayloadSize(config);

    manager = new DataSourceManager(config, DB_CONFIG_NAME);
  }

  @Override
  public void executeEach() throws SQLException {
    List<Integer> userIds = new ArrayList<>(opsPerTx);
    List<String> payloads = new ArrayList<>(opsPerTx);
    char[] payload = new char[payloadSize];
    for (int i = 0; i < opsPerTx; ++i) {
      userIds.add(ThreadLocalRandom.current().nextInt(recordCount));

      YcsbCommon.randomFastChars(ThreadLocalRandom.current(), payload);
      payloads.add(new String(payload));
    }

    Connection connection = null;
    while (true) {
      connection = manager.getConnection();
      connection.setAutoCommit(false);
      try {
        for (int i = 0; i < userIds.size(); i++) {
          int userId = userIds.get(i);
          read(connection, userId);
          write(connection, userId, payloads.get(i));
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
    // manager.close();
    setState(
        Json.createObjectBuilder()
            .add("transaction-retry-count", transactionRetryCount.toString())
            .build());
  }
}
