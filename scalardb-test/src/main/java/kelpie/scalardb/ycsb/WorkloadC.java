package kelpie.scalardb.ycsb;

import static kelpie.scalardb.ycsb.Common.CONFIG_NAME;
import static kelpie.scalardb.ycsb.Common.NAMESPACE;
import static kelpie.scalardb.ycsb.Common.OPS_PER_TX;
import static kelpie.scalardb.ycsb.Common.TABLE;
import static kelpie.scalardb.ycsb.Common.getRecordCount;
import static kelpie.scalardb.ycsb.Common.prepareGet;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.concurrent.ThreadLocalRandom;
import kelpie.scalardb.Common;

/** Workload C: Read only. */
public class WorkloadC extends TimeBasedProcessor {
  private static final long DEFAULT_OPS_PER_TX = 2; // two read operations
  private final DistributedTransactionManager manager;
  private final int recordCount;
  private final int opsPerTx;

  public WorkloadC(Config config) {
    super(config);
    this.manager = Common.getTransactionManager(config, NAMESPACE, TABLE);
    this.recordCount = getRecordCount(config);
    this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);
  }

  @Override
  public void executeEach() throws TransactionException {
    DistributedTransaction transaction = manager.start();

    try {
      for (int i = 0; i < opsPerTx; ++i) {
        transaction.get(prepareGet(ThreadLocalRandom.current().nextInt(recordCount)));
      }
      transaction.commit();
    } catch (Exception e) {
      transaction.abort();
    }
  }

  @Override
  public void close() throws Exception {
    manager.close();
  }
}
