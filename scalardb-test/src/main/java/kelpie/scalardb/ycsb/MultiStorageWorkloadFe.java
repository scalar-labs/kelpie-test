package kelpie.scalardb.ycsb;

import static kelpie.scalardb.ycsb.YcsbCommon.CONFIG_NAME;
import static kelpie.scalardb.ycsb.YcsbCommon.NAMESPACE_PRIMARY;
import static kelpie.scalardb.ycsb.YcsbCommon.NAMESPACE_SECONDARY;
import static kelpie.scalardb.ycsb.YcsbCommon.OPS_PER_TX;
import static kelpie.scalardb.ycsb.YcsbCommon.getPayloadSize;
import static kelpie.scalardb.ycsb.YcsbCommon.getRecordCount;
import static kelpie.scalardb.ycsb.YcsbCommon.prepareGet;
import static kelpie.scalardb.ycsb.YcsbCommon.preparePut;

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
 * Multi-storage workload Fe:
 * Same number of read-modify-write operation for both primary and secondary database.
*/
public class MultiStorageWorkloadFe extends TimeBasedProcessor {
  // one read-modify-write operation (one read and one write for the same record is regarded as one
  // operation)
  private static final long DEFAULT_OPS_PER_TX = 1;
  private final DistributedTransactionManager manager;
  private final int recordCount;
  private final int opsPerTx;
  private final int payloadSize;

  private final LongAdder transactionRetryCount = new LongAdder();

  public MultiStorageWorkloadFe(Config config) {
    super(config);
    this.manager = Common.getTransactionManager(config);
    this.recordCount = getRecordCount(config);
    this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);
    this.payloadSize = getPayloadSize(config);
  }

  @Override
  public void executeEach() throws TransactionException {
    List<Integer> primaryIds = new ArrayList<>(opsPerTx);
    List<Integer> secondaryIds = new ArrayList<>(opsPerTx);
    List<String> payloads = new ArrayList<>(opsPerTx);
    char[] payload = new char[payloadSize];
    for (int i = 0; i < opsPerTx; ++i) {
      primaryIds.add(ThreadLocalRandom.current().nextInt(recordCount));

      YcsbCommon.randomFastChars(ThreadLocalRandom.current(), payload);
      payloads.add(new String(payload));
    }
    for (int i = 0; i < opsPerTx; ++i) {
      secondaryIds.add(ThreadLocalRandom.current().nextInt(recordCount));
      // use same payload for secondary
    }

    while (true) {
      DistributedTransaction transaction = manager.start();
      try {
        for (int i = 0; i < primaryIds.size(); i++) {
          int userId = primaryIds.get(i);
          transaction.get(prepareGet(NAMESPACE_PRIMARY, userId));
          transaction.put(preparePut(NAMESPACE_PRIMARY, userId, payloads.get(i)));
        }
        for (int i = 0; i < secondaryIds.size(); i++) {
          int userId = secondaryIds.get(i);
          transaction.get(prepareGet(NAMESPACE_SECONDARY, userId));
          transaction.put(preparePut(NAMESPACE_SECONDARY, userId, payloads.get(i)));
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
