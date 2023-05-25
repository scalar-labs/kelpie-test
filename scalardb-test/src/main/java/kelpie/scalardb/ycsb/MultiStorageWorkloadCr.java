package kelpie.scalardb.ycsb;

import static kelpie.scalardb.ycsb.YcsbCommon.CONFIG_NAME;
import static kelpie.scalardb.ycsb.YcsbCommon.NAMESPACE_PRIMARY;
import static kelpie.scalardb.ycsb.YcsbCommon.NAMESPACE_SECONDARY;
import static kelpie.scalardb.ycsb.YcsbCommon.OPS_PER_TX;
import static kelpie.scalardb.ycsb.YcsbCommon.getRecordCount;
import static kelpie.scalardb.ycsb.YcsbCommon.getDispatchRate;
import static kelpie.scalardb.ycsb.YcsbCommon.prepareGet;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import javax.json.Json;
import kelpie.scalardb.Common;

/**
 * Multi-storage workload Cr:
 * Read operation for either database probablistically based on the specified rate.
*/
public class MultiStorageWorkloadCr extends TimeBasedProcessor {
  private static final long DEFAULT_OPS_PER_TX = 1;
  private final DistributedTransactionManager manager;
  private final int recordCount;
  private final int opsPerTx;
  private final int dispatchRate;

  private final LongAdder transactionRetryCount = new LongAdder();

  public MultiStorageWorkloadCr(Config config) {
    super(config);
    this.manager = Common.getTransactionManager(config);
    this.recordCount = getRecordCount(config);
    this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);
    this.dispatchRate = getDispatchRate(config);
  }

  @Override
  public void executeEach() throws TransactionException {
    List<Integer> userIds = new ArrayList<>(opsPerTx);
    for (int i = 0; i < opsPerTx; ++i) {
      userIds.add(ThreadLocalRandom.current().nextInt(recordCount));
    }

    while (true) {
      DistributedTransaction transaction = manager.start();
      try {
        for (int i = 0; i < userIds.size(); i++) {
          int userId = userIds.get(i);
          int rate = ThreadLocalRandom.current().nextInt(100) + 1;
          if (rate > dispatchRate) {
            transaction.get(prepareGet(NAMESPACE_PRIMARY, userId));
          } else {
            transaction.get(prepareGet(NAMESPACE_SECONDARY, userId));
          }
        }
        transaction.commit();
        break;
      } catch (CrudConflictException | CommitConflictException e) {
        transaction.abort();
        transactionRetryCount.increment();
      } catch (Exception e) {
        transaction.abort();
        throw e;
      }
    }
  }

  @Override
  public void close() throws Exception {
    manager.close();
    setState(
        Json.createObjectBuilder()
            .add("transaction-retry-count", transactionRetryCount.toString())
            .build());
  }
}
