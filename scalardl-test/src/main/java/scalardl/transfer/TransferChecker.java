package scalardl.transfer;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.db.api.TransactionState;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CoordinatorException;
import com.scalar.db.service.StorageModule;
import com.scalar.db.service.StorageService;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.dl.client.service.ClientService;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.exception.PostProcessException;
import com.scalar.kelpie.modules.PostProcessor;
import io.github.resilience4j.retry.Retry;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import scalardl.Common;

public class TransferChecker extends PostProcessor {

  public TransferChecker(Config config) {
    super(config);
  }

  @Override
  public void execute() {
    List<JsonObject> results = readBalancesWithRetry();

    int committed = getNumOfCommittedFromCoordinator();

    if (!isConsistent(results, committed)) {
      throw new PostProcessException("Inconsistency happened!");
    }
  }

  private List<JsonObject> readBalancesWithRetry() {
    logInfo("reading latest assets...");
    int maxRetry = (int) config.getUserLong("test_config", "checker_max_retries_for_read", 10L);
    long retryIntervalSleepTime =
        config.getUserLong("test_config", "checker_retry_interval_millis", 1000L);
    Retry retry = Common.getRetryWithExponentialBackoff("readBalances", maxRetry, retryIntervalSleepTime);
    Supplier<List<JsonObject>> decorated = Retry.decorateSupplier(retry, this::readBalances);

    try {
      return decorated.get();
    } catch (Exception e) {
      throw new PostProcessException("Reading records failed repeatedly", e);
    }
  }

  @Override
  public void close() {}

  private List<JsonObject> readBalances() {
    int numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    List<JsonObject> results = new ArrayList<>();

    boolean isFailed = false;
    ClientService service = Common.getClientService(config);
    String name = config.getUserString("contract", "balance_contract_name");

    for (int i = 0; i < numAccounts; i++) {
      try {
        JsonObject argument =
            Json.createObjectBuilder()
                .add("asset_id", String.valueOf(i))
                .add("nonce", UUID.randomUUID().toString())
                .build();

        JsonObject result = service.executeContract(name, argument).getResult().get();
        results.add(result);
      } catch (Exception e) {
        // continue to read other records
        logWarn("failed to read asset_id " + i, e);
        isFailed = true;
      }
    }

    if (isFailed) {
      // for Retry
      throw new RuntimeException("at least 1 record couldn't be read");
    }

    return results;
  }

  private int getNumOfCommittedFromCoordinator() {
    Coordinator coordinator = getCoordinator();
    Retry retry = Common.getRetryWithExponentialBackoff("checkCoordinator");
    Function<String, Optional<Coordinator.State>> decorated =
        Retry.decorateFunction(retry, id -> getState(coordinator, id));

    JsonObject unknownTransactions = getPreviousState().getJsonObject("unknown_transaction");
    if (unknownTransactions == null) {
      // for --only-post
      return 0;
    }
    int committed = 0;
    for (String txId : unknownTransactions.keySet()) {
      Optional<Coordinator.State> state;
      try {
        state = decorated.apply(txId);
      } catch (Exception e) {
        throw new PostProcessException("Reading the status failed repeatedly", e);
      }
      if (state.isPresent() && state.get().getState().equals(TransactionState.COMMITTED)) {
        JsonArray ids = unknownTransactions.getJsonArray(txId);
        logInfo(
            "id: "
                + txId
                + " from: "
                + ids.getInt(0)
                + " to: "
                + ids.getInt(1)
                + " succeeded, not failed");
        committed++;
      }
    }

    return committed;
  }

  private Coordinator getCoordinator() {
    Properties props = new Properties();
    String contactPoints = config.getUserString("storage_config", "contact_points");
    String username = config.getUserString("storage_config", "username", "cassandra");
    String password = config.getUserString("storage_config", "password", "cassandra");
    String storage = config.getUserString("storage_config", "storage", "cassandra");
    String prefix = config.getUserString("storage_config", "namespace_prefix", "");
    props.setProperty("scalar.db.contact_points", contactPoints);
    props.setProperty("scalar.db.username", username);
    props.setProperty("scalar.db.password", password);
    props.setProperty("scalar.db.storage", storage);
    props.setProperty("scalar.db.namespace_prefix", prefix);

    DatabaseConfig dbConfig = new DatabaseConfig(props);
    Injector injector = Guice.createInjector(new StorageModule(dbConfig));
    StorageService storageService = injector.getInstance(StorageService.class);

    return new Coordinator(storageService);
  }

  private Optional<Coordinator.State> getState(Coordinator coordinator, String txId) {
    try {
      logInfo("reading the status of " + txId);

      return coordinator.getState(txId);
    } catch (CoordinatorException e) {
      // convert the exception for Retry
      throw new RuntimeException("Failed to read the state from the coordinator", e);
    }
  }

  private boolean isConsistent(List<JsonObject> results, int committed) {
    int totalVersion = results.stream().mapToInt(r -> r.getInt("age")).sum();
    int totalBalance = results.stream().mapToInt(r -> r.getInt("balance")).sum();
    int expectedTotalVersion = ((int) getStats().getSuccessCount() + committed) * 2;
    int expectedTotalBalance = Common.getTotalInitialBalance(config);

    logInfo("total version: " + totalVersion);
    logInfo("expected total version: " + expectedTotalVersion);
    logInfo("total balance: " + totalBalance);
    logInfo("expected total balance: " + expectedTotalBalance);

    if (totalVersion != expectedTotalVersion) {
      logError("version mismatch !");
      return false;
    }
    if (totalBalance != expectedTotalBalance) {
      logError("balance mismatch !");
      return false;
    }
    return true;
  }
}
