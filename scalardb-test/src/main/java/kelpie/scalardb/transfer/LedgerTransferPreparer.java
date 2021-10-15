package kelpie.scalardb.transfer;

import static kelpie.scalardb.transfer.LedgerTransferCommon.CONFIG_TABLE_NAME;
import static kelpie.scalardb.transfer.LedgerTransferCommon.DEFAULT_METADATA_SIZE;

import com.google.common.base.Strings;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
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
import java.util.stream.IntStream;
import kelpie.scalardb.Common;

public class LedgerTransferPreparer extends PreProcessor {
  private static final long DEFAULT_POPULATION_CONCURRENCY = 32L;
  private static final long DEFAULT_POPULATION_NUM_ACCOUNTS_PER_TX = 100;
  private static final long DEFAULT_POPULATION_MAX_RETRIES = 10;
  private static final long DEFAULT_POPULATION_WAIT_MILLS = 1000;
  private final Retry retry;
  private final int metadataSize;

  private final DistributedTransactionManager manager;

  public LedgerTransferPreparer(Config config) {
    super(config);
    this.manager = LedgerTransferCommon.getTransactionManager(config);
    int maxRetries =
        (int)
            config.getUserLong(
                CONFIG_TABLE_NAME, "population_max_retries", DEFAULT_POPULATION_MAX_RETRIES);
    int waitMillis =
        (int)
            config.getUserLong(
                CONFIG_TABLE_NAME, "population_wait_millis", DEFAULT_POPULATION_WAIT_MILLS);
    this.retry = Common.getRetryWithFixedWaitDuration("populate", maxRetries, waitMillis);
    this.metadataSize =
        (int) config.getUserLong(CONFIG_TABLE_NAME, "metadata_size", DEFAULT_METADATA_SIZE);
  }

  @Override
  public void execute() {
    logInfo("insert initial values... ");

    int concurrency =
        (int)
            config.getUserLong(
                CONFIG_TABLE_NAME, "population_concurrency", DEFAULT_POPULATION_CONCURRENCY);
    ExecutorService es = Executors.newCachedThreadPool();
    List<CompletableFuture> futures = new ArrayList<>();
    IntStream.range(0, concurrency)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(() -> new PopulationRunner(i).run(), es);
              futures.add(future);
            });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
    logInfo("all records have been inserted");
  }

  @Override
  public void close() {
    manager.close();
  }

  private class PopulationRunner {
    private final int id;

    public PopulationRunner(int threadId) {
      this.id = threadId;
    }

    public void run() {
      int concurrency =
          (int)
              config.getUserLong(
                  CONFIG_TABLE_NAME, "population_concurrency", DEFAULT_POPULATION_CONCURRENCY);
      int numAccountsPerTx =
          (int)
              config.getUserLong(
                  CONFIG_TABLE_NAME,
                  "population_num_accounts_per_tx",
                  DEFAULT_POPULATION_NUM_ACCOUNTS_PER_TX);
      int numAccounts = (int) config.getUserLong(CONFIG_TABLE_NAME, "num_accounts");
      int numPerThread = (numAccounts + concurrency - 1) / concurrency;
      int start = numPerThread * id;
      int end = Math.min(numPerThread * (id + 1), numAccounts);
      IntStream.range(0, (numPerThread + numAccountsPerTx - 1) / numAccountsPerTx)
          .forEach(
              i -> {
                int startId = start + numAccountsPerTx * i;
                int endId = Math.min(start + numAccountsPerTx * (i + 1), end);
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
                String metadata = Strings.repeat("*", metadataSize);
                Put put =
                    LedgerTransferCommon.preparePut(
                        i, 0, LedgerTransferCommon.INITIAL_BALANCE, metadata);
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
