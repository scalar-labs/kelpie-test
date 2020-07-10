package scalardb;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageModule;
import com.scalar.db.service.StorageService;
import com.scalar.db.service.TransactionModule;
import com.scalar.db.service.TransactionService;
import com.scalar.db.transaction.consensuscommit.TransactionResult;
import com.scalar.kelpie.config.Config;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Common {
  private static final String KEYSPACE = "transfer";
  private static final String TABLE = "tx_transfer";
  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";
  private static final Duration WAIT_DURATION = Duration.ofMillis(1000);
  private static final long SLEEP_BASE_MILLIS = 100L;
  private static final int MAX_RETRIES = 10;

  public static final String DEFAULT_CONTACT_POINT = "localhost";
  public static final int INITIAL_BALANCE = 10000;
  public static final int NUM_TYPES = 2;

  public static DistributedStorage getStorage(Config config) {
    DatabaseConfig dbConfig = getDatabaseConfig(config);
    Injector injector = Guice.createInjector(new StorageModule(dbConfig));

    return injector.getInstance(StorageService.class);
  }

  public static DistributedTransactionManager getTransactionManager(Config config) {
    DatabaseConfig dbConfig = getDatabaseConfig(config);
    Injector injector = Guice.createInjector(new TransactionModule(dbConfig));
    DistributedTransactionManager manager = injector.getInstance(TransactionService.class);
    manager.with(KEYSPACE, TABLE);

    return manager;
  }

  private static DatabaseConfig getDatabaseConfig(Config config) {
    Properties props = new Properties();
    String contactPoints =
        config.getUserString("storage_config", "contact_points", DEFAULT_CONTACT_POINT);
    String username = config.getUserString("storage_config", "username", "cassandra");
    String password = config.getUserString("storage_config", "password", "cassandra");
    String storage = config.getUserString("storage_config", "storage", "cassandra");
    props.setProperty("scalar.db.contact_points", contactPoints);
    props.setProperty("scalar.db.username", username);
    props.setProperty("scalar.db.password", password);
    props.setProperty("scalar.db.storage", storage);

    return new DatabaseConfig(props);
  }

  public static Get prepareGet(int id, int type) {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
    Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, type));

    return new Get(partitionKey, clusteringKey).withConsistency(Consistency.LINEARIZABLE);
  }

  public static Put preparePut(int id, int type, int amount) {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
    Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, type));
    return new Put(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .withValue(new IntValue(BALANCE, amount));
  }

  public static int getBalanceFromResult(Result result) {
    return ((IntValue) result.getValue(BALANCE).get()).get();
  }

  public static int getTotalInitialBalance(Config config) {
    int numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    return INITIAL_BALANCE * NUM_TYPES * numAccounts;
  }

  public static int getActualTotalVersion(List<Result> results) {
    return results.stream().mapToInt(r -> ((TransactionResult) r).getVersion() - 1).sum();
  }

  public static int getActualTotalBalance(List<Result> results) {
    return results.stream().mapToInt(r -> ((IntValue) r.getValue(BALANCE).get()).get()).sum();
  }

  public static Retry getRetryWithFixedWaitDuration(String name) {
    RetryConfig retryConfig =
        RetryConfig.custom().maxAttempts(MAX_RETRIES).waitDuration(WAIT_DURATION).build();

    return Retry.of(name, retryConfig);
  }

  public static Retry getRetryWithExponentialBackoff(String name) {
    IntervalFunction intervalFunc = IntervalFunction.ofExponentialBackoff(SLEEP_BASE_MILLIS, 2.0);

    RetryConfig retryConfig =
        RetryConfig.custom().maxAttempts(MAX_RETRIES).intervalFunction(intervalFunc).build();

    return Retry.of(name, retryConfig);
  }
}
