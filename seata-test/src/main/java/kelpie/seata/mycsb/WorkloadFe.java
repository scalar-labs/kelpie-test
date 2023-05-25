package kelpie.seata.mycsb;

import static kelpie.seata.mycsb.MycsbCommon.CONFIG_NAME;
import static kelpie.seata.mycsb.MycsbCommon.PRIMARY_DB_CONFIG_NAME;
import static kelpie.seata.mycsb.MycsbCommon.SECONDARY_DB_CONFIG_NAME;
import static kelpie.seata.mycsb.MycsbCommon.OPS_PER_TX;
import static kelpie.seata.mycsb.MycsbCommon.getPayloadSize;
import static kelpie.seata.mycsb.MycsbCommon.getRecordCount;
import static kelpie.seata.mycsb.MycsbCommon.read;
import static kelpie.seata.mycsb.MycsbCommon.write;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import io.seata.core.exception.TransactionException;
import io.seata.rm.RMClient;
import io.seata.tm.TMClient;
import io.seata.tm.api.GlobalTransaction;
import io.seata.tm.api.GlobalTransactionContext;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import javax.json.Json;
import kelpie.seata.Common;
import kelpie.seata.DataSourceManager;

public class WorkloadFe extends TimeBasedProcessor {
  private static final long DEFAULT_OPS_PER_TX = 1;
  private final DataSourceManager primary;
  private final DataSourceManager secondary;
  private final int recordCount;
  private final int opsPerTx;
  private final int payloadSize;

  private final LongAdder transactionRetryCount = new LongAdder();

  public WorkloadFe(Config config) {
    super(config);
    this.recordCount = getRecordCount(config);
    this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);
    this.payloadSize = getPayloadSize(config);

    TMClient.init(Common.APPLICATION_ID, Common.TRANSACTION_SERVICE_GROUP);
    RMClient.init(Common.APPLICATION_ID, Common.TRANSACTION_SERVICE_GROUP);
    primary = new DataSourceManager(config, PRIMARY_DB_CONFIG_NAME);
    secondary = new DataSourceManager(config, SECONDARY_DB_CONFIG_NAME);
  }

  @Override
  public void executeEach() throws SQLException, TransactionException {
    List<Integer> userIds = new ArrayList<>(opsPerTx);
    List<String> payloads = new ArrayList<>(opsPerTx);
    char[] payload = new char[payloadSize];
    for (int i = 0; i < opsPerTx; ++i) {
      userIds.add(ThreadLocalRandom.current().nextInt(recordCount));

      MycsbCommon.randomFastChars(ThreadLocalRandom.current(), payload);
      payloads.add(new String(payload));
    }

    Connection primaryConnection = null;
    Connection secondaryConnection = null;
    while (true) {
      GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();
      primaryConnection = primary.getConnectionXA();
      secondaryConnection = secondary.getConnectionXA();  
      try {
        tx.begin();
        for (int i = 0; i < userIds.size(); i++) {
          int userId = userIds.get(i);
          read(primaryConnection, userId);
          write(primaryConnection, userId, payloads.get(i));
        }
        for (int i = 0; i < userIds.size(); i++) {
          int userId = userIds.get(i);
          read(secondaryConnection, userId);
          write(secondaryConnection, userId, payloads.get(i));
        }
        tx.commit();
        break;
      } catch (SQLException | TransactionException e) {
        e.printStackTrace();
        tx.rollback();
        transactionRetryCount.increment();
      } catch (Exception e) {
        tx.rollback();
        throw e;
      } finally {
        if (primaryConnection != null) {
          primaryConnection.close();
        }
        if (secondaryConnection != null) {
          secondaryConnection.close();
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
