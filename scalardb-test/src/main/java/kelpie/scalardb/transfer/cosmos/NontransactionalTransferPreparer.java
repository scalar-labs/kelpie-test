package kelpie.scalardb.transfer.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
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
import kelpie.scalardb.transfer.TransferCommon;

public class NontransactionalTransferPreparer extends PreProcessor {
  private static final long DEFAULT_POPULATION_CONCURRENCY = 32L;
  private static final int NUM_ACCOUNTS_PER_TX = 100;
  private static final int COSMOS_STATUS_CONFLICT = 409;
  private final CosmosClient client;
  private final CosmosContainer container;

  public NontransactionalTransferPreparer(Config config) {
    super(config);

    client = CosmosUtil.createCosmosClient(config);
    container = client.getDatabase(TransferCommon.KEYSPACE).getContainer(TransferCommon.TABLE);
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
  public void close() {
    client.close();
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
      IntStream.range(0, (numPerThread + NUM_ACCOUNTS_PER_TX - 1) / NUM_ACCOUNTS_PER_TX)
          .forEach(
              i -> {
                int startId = start + NUM_ACCOUNTS_PER_TX * i;
                int endId = Math.min(start + NUM_ACCOUNTS_PER_TX * (i + 1), end);
                populate(startId, endId);
              });
    }

    private void populate(int startId, int endId) {
      Runnable populate =
          () -> {
            for (int i = startId; i < endId; ++i) {
              for (int j = 0; j < TransferCommon.NUM_TYPES; ++j) {
                Account account = new Account();
                account.setId(i + ":" + j);
                account.setAccountId(i);
                account.setType(j);
                account.setBalance(TransferCommon.INITIAL_BALANCE);
                try {
                  container.createItem(
                      account,
                      new PartitionKey(account.getAccountId()),
                      new CosmosItemRequestOptions());
                  if (i % 100 == 0) {
                    logInfo(id + ": " + i + " items are inserted.");
                  }
                } catch (CosmosException e) {
                  if (e.getStatusCode() != COSMOS_STATUS_CONFLICT) {
                    throw new RuntimeException("population failed, retry", e);
                  }
                }
              }
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
