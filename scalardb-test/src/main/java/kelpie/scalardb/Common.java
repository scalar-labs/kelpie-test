package kelpie.scalardb;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.exception.IllegalConfigException;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Common {
  private static final Logger logger = LoggerFactory.getLogger(Common.class);

  private static final int WAIT_MILLS = 1000;
  private static final long SLEEP_BASE_MILLIS = 100L;
  private static final int MAX_RETRIES = 10;

  public static DistributedStorage getStorage(Config config) {
    DatabaseConfig dbConfig = getDatabaseConfig(config);
    StorageFactory factory = new StorageFactory(dbConfig);
    return factory.getStorage();
  }

  public static DistributedTransactionManager getTransactionManager(Config config) {
    DatabaseConfig dbConfig = getDatabaseConfig(config);
    TransactionFactory factory = new TransactionFactory(dbConfig);
    return factory.getTransactionManager();
  }

  public static TwoPhaseCommitTransactionManager getTwoPhaseCommitTransactionManager1(
      Config config) {
    return getTwoPhaseCommitTransactionManager(getDatabaseConfig1(config));
  }

  public static TwoPhaseCommitTransactionManager getTwoPhaseCommitTransactionManager2(
      Config config) {
    return getTwoPhaseCommitTransactionManager(getDatabaseConfig2(config));
  }

  private static TwoPhaseCommitTransactionManager getTwoPhaseCommitTransactionManager(
      Properties properties) {
    TransactionFactory factory = TransactionFactory.create(properties);
    return factory.getTwoPhaseCommitTransactionManager();
  }

  public static DatabaseConfig getDatabaseConfig(Config config) {
    String configFile;
    try {
      configFile = config.getUserString("storage_config", "config_file");
    } catch (IllegalConfigException e) {
      configFile = null;
    }
    if (configFile != null) {
      try {
        return new DatabaseConfig(new File(configFile));
      } catch (IOException e) {
        logger.warn("failed to load the specified config file: {}", configFile, e);
      }
    }

    String contactPoints = config.getUserString("storage_config", "contact_points", "localhost");
    long contactPort = config.getUserLong("storage_config", "contact_port", 0L);
    String username = config.getUserString("storage_config", "username", "cassandra");
    String password = config.getUserString("storage_config", "password", "cassandra");
    String storage = config.getUserString("storage_config", "storage", "cassandra");
    String isolationLevel = config.getUserString("storage_config", "isolation_level", "SNAPSHOT");
    String transactionManager =
        config.getUserString("storage_config", "transaction_manager", "consensus-commit");

    Properties props = new Properties();
    props.setProperty("scalar.db.contact_points", contactPoints);
    if (contactPort > 0) {
      props.setProperty("scalar.db.contact_port", Long.toString(contactPort));
    }
    props.setProperty("scalar.db.username", username);
    props.setProperty("scalar.db.password", password);
    props.setProperty("scalar.db.storage", storage);
    props.setProperty("scalar.db.transaction_manager", transactionManager);
    props.setProperty("scalar.db.isolation_level", isolationLevel);
    return new DatabaseConfig(props);
  }

  public static Properties getDatabaseConfig1(Config config) {
    return getDatabaseConfig(config, "config_file1");
  }

  public static Properties getDatabaseConfig2(Config config) {
    return getDatabaseConfig(config, "config_file2");
  }

  private static Properties getDatabaseConfig(Config config, String configName) {
    String configFile = config.getUserString("storage_config", configName);
    try {
      Properties ret = new Properties();
      try (FileInputStream stream = new FileInputStream(configFile)) {
        ret.load(stream);
      }
      return ret;
    } catch (IOException e) {
      throw new RuntimeException("failed to load the specified config file: " + configFile, e);
    }
  }

  public static boolean isCommitted(Coordinator coordinator, String txId) {
    Retry retry = Common.getRetryWithExponentialBackoff("checkCoordinator");
    Function<String, Optional<Coordinator.State>> decorated =
        Retry.decorateFunction(retry, id -> getState(coordinator, id));

    Optional<Coordinator.State> state;
    try {
      state = decorated.apply(txId);
    } catch (Exception e) {
      throw new RuntimeException("Reading the status failed repeatedly", e);
    }

    return state.isPresent() && state.get().getState().equals(TransactionState.COMMITTED);
  }

  private static Optional<Coordinator.State> getState(Coordinator coordinator, String txId) {
    try {
      return coordinator.getState(txId);
    } catch (Exception e) {
      // convert the exception for Retry
      throw new RuntimeException("Failed to read the state from the coordinator", e);
    }
  }

  public static Retry getRetryWithFixedWaitDuration(String name) {
    return getRetryWithFixedWaitDuration(name, MAX_RETRIES, WAIT_MILLS);
  }

  public static Retry getRetryWithFixedWaitDuration(String name, int maxRetries, int waitMillis) {
    RetryConfig retryConfig =
        RetryConfig.custom()
            .maxAttempts(maxRetries)
            .waitDuration(Duration.ofMillis(waitMillis))
            .build();

    return Retry.of(name, retryConfig);
  }

  public static Retry getRetryWithExponentialBackoff(String name) {
    return getRetryWithExponentialBackoff(name, MAX_RETRIES, SLEEP_BASE_MILLIS);
  }

  public static Retry getRetryWithExponentialBackoff(String name, int maxRetries, long waitMillis) {
    IntervalFunction intervalFunc = IntervalFunction.ofExponentialBackoff(waitMillis, 2.0);

    RetryConfig retryConfig =
        RetryConfig.custom().maxAttempts(maxRetries).intervalFunction(intervalFunc).build();

    return Retry.of(name, retryConfig);
  }
}
