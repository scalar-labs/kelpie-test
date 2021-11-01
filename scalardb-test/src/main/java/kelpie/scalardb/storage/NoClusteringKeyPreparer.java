package kelpie.scalardb.storage;

import static kelpie.scalardb.storage.NoClusteringKeySchema.preparePut;
import static kelpie.scalardb.storage.StorageCommon.DEFAULT_POPULATION_CONCURRENCY;

import com.scalar.db.api.DistributedStorage;
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
import kelpie.scalardb.transfer.TransferCommon;

public class NoClusteringKeyPreparer extends PreProcessor {

  private final DistributedStorage storage;

  public NoClusteringKeyPreparer(Config config) {
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
                  CompletableFuture.runAsync(() -> new PopulationRunner(i).run(), es);
              futures.add(future);
            });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    logInfo("all records have been inserted");
  }

  @Override
  public void close() throws Exception {
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
      int numKeys = (int) config.getUserLong("test_config", "num_keys");
      int numPerThread = (numKeys + concurrency - 1) / concurrency;
      int start = numPerThread * id;
      int end = Math.min(numPerThread * (id + 1), numKeys);
      IntStream.range(start, end).forEach(this::populate);
    }

    private void populate(int pkey) {
      Runnable populate =
          () -> {
            try {
              storage.put(preparePut(pkey, ThreadLocalRandom.current().nextInt()));
              NoClusteringKeyPreparer.this.logInfo("pkey=" + pkey + " inserted");
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
