package kelpie.scalardb.transfer;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PreProcessor;
import io.github.resilience4j.retry.Retry;
import kelpie.scalardb.Common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class TransferWithoutTransactionPreparer extends PreProcessor {
  private static final long DEFAULT_POPULATION_CONCURRENCY = 32L;
  private static final int NUM_ACCOUNTS_PER_THREAD = 100;

  private final DistributedStorage storage;

  public TransferWithoutTransactionPreparer(Config config) {
    super(config);
    this.storage = TransferCommon.getStorage(config);
  }

  @Override
  public void execute() {
    logInfo("insert initial values... ");

    int concurrency =
        (int)
            config.getUserLong(
                "test_config", "population_concurrency", DEFAULT_POPULATION_CONCURRENCY);
    ExecutorService es = Executors.newCachedThreadPool();
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    IntStream.range(0, concurrency)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(
                      () -> {
                        new PopulationRunner(i).run();
                      },
                      es);
              futures.add(future);
            });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
    logInfo("all records have been inserted");
  }

  @Override
  public void close() {
    storage.close();
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
                  "test_config", "population_concurrency", DEFAULT_POPULATION_CONCURRENCY);
      int numAccounts = (int) config.getUserLong("test_config", "num_accounts");
      int numPerThread = (numAccounts + concurrency - 1) / concurrency;
      int start = numPerThread * id;
      int end = Math.min(numPerThread * (id + 1), numAccounts);
      IntStream.range(0, (numPerThread + NUM_ACCOUNTS_PER_THREAD - 1) / NUM_ACCOUNTS_PER_THREAD)
          .forEach(
              i -> {
                int startId = start + NUM_ACCOUNTS_PER_THREAD * i;
                int endId = Math.min(start + NUM_ACCOUNTS_PER_THREAD * (i + 1), end);
                populate(startId, endId);
              });
    }

    private void populate(int startId, int endId) {
      Runnable populate =
          () -> {
            try {
              for (int i = startId; i < endId; ++i) {
                for (int j = 0; j < TransferCommon.NUM_TYPES; ++j) {
                  Put put = TransferCommon.preparePut(i, j, TransferCommon.INITIAL_BALANCE);
                  storage.put(put);
                }
              }
            } catch (Exception e) {
              throw new RuntimeException("population failed, retry", e);
            }
          };

      Retry retry = Common.getRetryWithFixedWaitDuration("populate");
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
