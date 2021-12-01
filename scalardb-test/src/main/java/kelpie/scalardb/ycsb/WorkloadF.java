package kelpie.scalardb.ycsb;

import static kelpie.scalardb.ycsb.YcsbCommon.CONFIG_NAME;
import static kelpie.scalardb.ycsb.YcsbCommon.NAMESPACE;
import static kelpie.scalardb.ycsb.YcsbCommon.OPS_PER_TX;
import static kelpie.scalardb.ycsb.YcsbCommon.TABLE;
import static kelpie.scalardb.ycsb.YcsbCommon.getPayloadSize;
import static kelpie.scalardb.ycsb.YcsbCommon.getRecordCount;
import static kelpie.scalardb.ycsb.YcsbCommon.prepareGet;
import static kelpie.scalardb.ycsb.YcsbCommon.preparePut;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.concurrent.ThreadLocalRandom;
import kelpie.scalardb.Common;

/** Workload F: Read-modify-write. */
public class WorkloadF extends TimeBasedProcessor {
  // one read-modify-write operation (one read and one write for the same record is regarded as one
  // operation)
  private static final long DEFAULT_OPS_PER_TX = 1;
  private final DistributedTransactionManager manager;
  private final int recordCount;
  private final int opsPerTx;
  private final char[] payload;

  public WorkloadF(Config config) {
    super(config);
    this.manager = Common.getTransactionManager(config, NAMESPACE, TABLE);
    this.recordCount = getRecordCount(config);
    this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);
    this.payload = new char[getPayloadSize(config)];
  }

  @Override
  public void executeEach() throws TransactionException {
    DistributedTransaction transaction = manager.start();

    try {
      for (int i = 0; i < opsPerTx; ++i) {
        int userId = ThreadLocalRandom.current().nextInt(recordCount);
        transaction.get(prepareGet(userId));
        YcsbCommon.randomFastChars(ThreadLocalRandom.current(), payload);
        transaction.put(preparePut(userId, new String(payload)));
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
