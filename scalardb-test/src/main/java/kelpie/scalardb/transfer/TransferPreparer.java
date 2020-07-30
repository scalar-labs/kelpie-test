package kelpie.scalardb.transfer;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Put;
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

public class TransferPreparer extends PreProcessor {
  private static final long DEFAULT_POPULATION_CONCURRENCY = 32L;
  private static final int NUM_ACCOUNTS_PER_TX = 100;

  private final DistributedTransactionManager manager;

  public TransferPreparer(Config config) {
    super(config);
    this.manager = TransferCommon.getTransactionManager(config);
  }

  @Override
  public void execute() {
    logInfo("insert initial values... ");

    int concurrency =
        (int)
            config.getUserLong(
                "test_config", "population_concurrency", DEFAULT_POPULATION_CONCURRENCY);
    ExecutorService es = Executors.newCachedThreadPool();
    List<CompletableFuture> futures = new ArrayList<>();
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
  public void close() {}

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
      IntStream.range(0, (numPerThread + NUM_ACCOUNTS_PER_TX - 1) / NUM_ACCOUNTS_PER_TX)
          .forEach(
              i -> {
                int startId = start + NUM_ACCOUNTS_PER_TX * i;
                int endId = Math.min(start + NUM_ACCOUNTS_PER_TX * (i + 1), end);
                populateWithTx(startId, endId);
              });
    }

    private void populateWithTx(int startId, int endId) {
      Runnable populate =
          () -> {
            DistributedTransaction transaction = manager.start();
            IntStream.range(startId, endId)
                .forEach(
                    i -> {
                      IntStream.range(0, TransferCommon.NUM_TYPES)
                          .forEach(
                              j -> {
                                Put put =
                                    TransferCommon.preparePut(i, j, TransferCommon.INITIAL_BALANCE);
                                transaction.put(put);
                              });
                    });
            try {
              transaction.commit();
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
