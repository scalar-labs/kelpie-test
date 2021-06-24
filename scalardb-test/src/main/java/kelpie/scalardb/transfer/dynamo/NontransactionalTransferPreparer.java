package kelpie.scalardb.transfer.dynamo;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PreProcessor;
import io.github.resilience4j.retry.Retry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import kelpie.scalardb.Common;
import kelpie.scalardb.transfer.TransferCommon;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

public class NontransactionalTransferPreparer extends PreProcessor {
  private static final long DEFAULT_POPULATION_CONCURRENCY = 32L;
  private static final int NUM_ACCOUNTS_PER_TX = 100;
  private final DynamoDbClient client;

  public NontransactionalTransferPreparer(Config config) {
    super(config);

    client = DynamoUtil.createDynamoClient(config);
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
                Map<String, AttributeValue> values = new HashMap<>();
                values.put("account_id", AttributeValue.builder().n(Integer.toString(i)).build());
                values.put("account_type", AttributeValue.builder().n(Integer.toString(j)).build());
                values.put(
                    "balance",
                    AttributeValue.builder()
                        .n(Integer.toString(TransferCommon.INITIAL_BALANCE))
                        .build());
                PutItemRequest request =
                    PutItemRequest.builder().tableName("transfer").item(values).build();
                try {
                  client.putItem(request);

                  if (i % 100 == 0) {
                    logInfo(id + ": " + i + " items are inserted.");
                  }
                } catch (DynamoDbException e) {
                  throw new RuntimeException("population failed, retry", e);
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
