package kelpie.seata.mycsb;

import static kelpie.seata.mycsb.MycsbCommon.CONFIG_NAME;
import static kelpie.seata.mycsb.MycsbCommon.PRIMARY_DB_CONFIG_NAME;
import static kelpie.seata.mycsb.MycsbCommon.SECONDARY_DB_CONFIG_NAME;
import static kelpie.seata.mycsb.MycsbCommon.OPS_PER_TX;
import static kelpie.seata.mycsb.MycsbCommon.getRecordCount;
import static kelpie.seata.mycsb.MycsbCommon.getHotspotRecordCount;
import static kelpie.seata.mycsb.MycsbCommon.read;

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

public class WorkloadCh extends TimeBasedProcessor {
  private static final long DEFAULT_OPS_PER_TX = 1;
  private final DataSourceManager primary;
  private final DataSourceManager secondary;
  private final int recordCount;
  private final int hotspotRecordCount;
  private final int opsPerTx;

  private final LongAdder transactionRetryCount = new LongAdder();

  public WorkloadCh(Config config) {
    super(config);
    this.recordCount = getRecordCount(config);
    this.hotspotRecordCount = getHotspotRecordCount(config);
    this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);

    TMClient.init(Common.APPLICATION_ID, Common.TRANSACTION_SERVICE_GROUP);
    RMClient.init(Common.APPLICATION_ID, Common.TRANSACTION_SERVICE_GROUP);
    primary = new DataSourceManager(config, PRIMARY_DB_CONFIG_NAME);
    secondary = new DataSourceManager(config, SECONDARY_DB_CONFIG_NAME);
  }

  @Override
  public void executeEach() throws SQLException, TransactionException {
    List<Integer> primaryIds = new ArrayList<>(opsPerTx);
    List<Integer> secondaryIds = new ArrayList<>(opsPerTx);
    for (int i = 0; i < opsPerTx; ++i) {
      primaryIds.add(ThreadLocalRandom.current().nextInt(hotspotRecordCount));
    }
    for (int i = 0; i < opsPerTx; ++i) {
      secondaryIds.add(ThreadLocalRandom.current().nextInt(recordCount));
    }

    Connection primaryConnection = null;
    Connection secondaryConnection = null;
    while (true) {
      GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();
      primaryConnection = primary.getConnectionXA();
      secondaryConnection = secondary.getConnectionXA();  
      try {
        tx.begin();
        for (int i = 0; i < primaryIds.size(); i++) {
          int userId = primaryIds.get(i);
          read(primaryConnection, userId);
        }
        for (int i = 0; i < secondaryIds.size(); i++) {
          int userId = secondaryIds.get(i);
          read(secondaryConnection, userId);
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
