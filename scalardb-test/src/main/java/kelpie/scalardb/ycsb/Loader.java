package kelpie.scalardb.ycsb;

import static kelpie.scalardb.ycsb.YcsbCommon.CONFIG_NAME;
import static kelpie.scalardb.ycsb.YcsbCommon.NAMESPACE;
import static kelpie.scalardb.ycsb.YcsbCommon.TABLE;
import static kelpie.scalardb.ycsb.YcsbCommon.getPayloadSize;
import static kelpie.scalardb.ycsb.YcsbCommon.getRecordCount;
import static kelpie.scalardb.ycsb.YcsbCommon.prepareGet;
import static kelpie.scalardb.ycsb.YcsbCommon.preparePut;
import static kelpie.scalardb.ycsb.YcsbCommon.randomFastChars;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PreProcessor;
import io.github.resilience4j.retry.Retry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import kelpie.scalardb.Common;

public class Loader extends PreProcessor {
  private static final long DEFAULT_POPULATION_CONCURRENCY = 10L;
  private static final long DEFAULT_BATCH_SIZE = 1;
  private static final String POPULATION_CONCURRENCY = "population_concurrency";
  private static final String POPULATION_READ_BEFORE_WRITE = "population_read_before_write";
  private static final String BATCH_SIZE = "batch_size";
  private static final int MAX_RETRIES = 10;
  private static final int WAIT_DURATION_MILLIS = 1000;
  private final DistributedTransactionManager manager;
  private final int concurrency;
  private final int recordCount;
  private final char[] payload;
  private final int batchSize;
  private final boolean readBeforeWrite;

  public Loader(Config config) {
    super(config);
    manager = Common.getTransactionManager(config, NAMESPACE, TABLE);
    concurrency =
        (int)
            config.getUserLong(CONFIG_NAME, POPULATION_CONCURRENCY, DEFAULT_POPULATION_CONCURRENCY);
    batchSize = (int) config.getUserLong(CONFIG_NAME, BATCH_SIZE, DEFAULT_BATCH_SIZE);
    recordCount = getRecordCount(config);
    payload = new char[getPayloadSize(config)];
    readBeforeWrite = config.getUserBoolean(CONFIG_NAME, POPULATION_READ_BEFORE_WRITE, false);
  }

  @Override
  public void execute() {
    ExecutorService es = Executors.newCachedThreadPool();
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    IntStream.range(0, concurrency)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(() -> new PopulationRunner(i).run(), es);
              futures.add(future);
            });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    logInfo("all records have been inserted");
  }

  @Override
  public void close() throws Exception {
    try {
      manager.close();
    } catch (Exception e) {
      logWarn("Failed to close the transaction manager", e);
    }
  }

  private class PopulationRunner {
    private final int id;

    public PopulationRunner(int threadId) {
      this.id = threadId;
    }

    public void run() {
      int numPerThread = (recordCount + concurrency - 1) / concurrency;
      int start = numPerThread * id;
      int end = Math.min(numPerThread * (id + 1), recordCount);
      IntStream.range(0, (numPerThread + batchSize - 1) / batchSize)
          .forEach(
              i -> {
                int startId = start + batchSize * i;
                int endId = Math.min(start + batchSize * (i + 1), end);
                populateWithTx(startId, endId);
              });
    }

    private void populateWithTx(int startId, int endId) {
      Runnable populate =
          () -> {
            DistributedTransaction transaction = null;
            try {
              transaction = manager.start();
              for (int i = startId; i < endId; ++i) {
                if (readBeforeWrite) {
                  Get get = prepareGet(i);
                  transaction.get(get);
                }
                randomFastChars(ThreadLocalRandom.current(), payload);
                Put put = preparePut(i, new String(payload));
                transaction.put(put);
              }
              transaction.commit();
            } catch (Exception e) {
              if (transaction != null) {
                try {
                  transaction.abort();
                } catch (AbortException ex) {
                  logWarn("abort failed.", ex);
                }
              }
              logWarn("population failed.", e);
              throw new RuntimeException("population failed.", e);
            }
          };

      Retry retry =
          Common.getRetryWithFixedWaitDuration("populate", MAX_RETRIES, WAIT_DURATION_MILLIS);
      Runnable decorated = Retry.decorateRunnable(retry, populate);
      try {
        decorated.run();
      } catch (Exception e) {
        logError("population failed repeatedly!");
        throw e;
      }
    }
  }
}
