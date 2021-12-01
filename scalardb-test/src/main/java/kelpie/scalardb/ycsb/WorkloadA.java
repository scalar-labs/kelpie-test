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

/**
 * Workload A: Update heavy workload. This workload has a mix of 50/50 reads and writes. The writes
 * can be changed to read-modify-write if "use_read_modify_write" is set to true.
 */
public class WorkloadA extends TimeBasedProcessor {
  private static final long DEFAULT_OPS_PER_TX = 2; // one read operation and one write operation
  private static final String USE_READ_MODIFY_WRITE = "use_read_modify_write";
  private final DistributedTransactionManager manager;
  private final int recordCount;
  private final int opsPerTx;
  private final boolean useReadModifyWrite;
  private final char[] payload;

  public WorkloadA(Config config) {
    super(config);
    this.manager = Common.getTransactionManager(config, NAMESPACE, TABLE);
    this.recordCount = getRecordCount(config);
    this.payload = new char[getPayloadSize(config)];
    this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);
    if (opsPerTx % 2 != 0) {
      throw new IllegalArgumentException(OPS_PER_TX + " must be a multiple of 2.");
    }
    useReadModifyWrite = config.getUserBoolean(CONFIG_NAME, USE_READ_MODIFY_WRITE, false);
  }

  @Override
  public void executeEach() throws TransactionException {
    DistributedTransaction transaction = manager.start();

    int readOpsPerTx = opsPerTx / 2;
    int writeOpsPerTx = opsPerTx / 2;

    try {
      for (int i = 0; i < readOpsPerTx; ++i) {
        transaction.get(prepareGet(ThreadLocalRandom.current().nextInt(recordCount)));
      }

      for (int i = 0; i < writeOpsPerTx; ++i) {
        int userId = ThreadLocalRandom.current().nextInt(recordCount);
        if (useReadModifyWrite) {
          transaction.get(prepareGet(userId));
        }
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
