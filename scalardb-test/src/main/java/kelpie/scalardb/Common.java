package kelpie.scalardb;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CoordinatorException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.rpc.GrpcConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.kelpie.config.Config;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

public class Common {
  private static final int WAIT_MILLS = 1000;
  private static final long SLEEP_BASE_MILLIS = 100L;
  private static final int MAX_RETRIES = 10;

  public static DistributedStorage getStorage(Config config) {
    DatabaseConfig dbConfig = getDatabaseConfig(config);
    StorageFactory factory = new StorageFactory(dbConfig);
    return factory.getStorage();
  }

  public static DistributedTransactionManager getTransactionManager(
      Config config, String keyspace, String table) {
    DatabaseConfig dbConfig = getDatabaseConfig(config);
    TransactionFactory factory = new TransactionFactory(dbConfig);
    DistributedTransactionManager manager = factory.getTransactionManager();
    manager.with(keyspace, table);
    return manager;
  }

  public static TwoPhaseCommitTransactionManager getTwoPhaseCommitTransactionManager(
      Config config, String keyspace, String table) {
    DatabaseConfig dbConfig = getDatabaseConfig(config);
    TransactionFactory factory = new TransactionFactory(dbConfig);
    TwoPhaseCommitTransactionManager manager = factory.getTwoPhaseCommitTransactionManager();
    manager.with(keyspace, table);
    return manager;
  }

  public static DatabaseConfig getDatabaseConfig(Config config) {
    String contactPoints = config.getUserString("storage_config", "contact_points", "localhost");
    long contactPort = config.getUserLong("storage_config", "contact_port", 0L);
    String username = config.getUserString("storage_config", "username", "cassandra");
    String password = config.getUserString("storage_config", "password", "cassandra");
    String storage = config.getUserString("storage_config", "storage", "cassandra");
    String isolationLevel = config.getUserString("storage_config", "isolation_level", "SNAPSHOT");
    String transactionManager =
        config.getUserString("storage_config", "transaction_manager", "consensus-commit");
    String serializableStrategy =
        config.getUserString("storage_config", "serializable_strategy", "EXTRA_READ");

    // JDBC adapter related configurations
    long jdbcConnectionPoolMinIdle =
        config.getUserLong(
            "storage_config",
            "jdbc_connection_pool_min_idle",
            (long) JdbcConfig.DEFAULT_CONNECTION_POOL_MIN_IDLE);
    long jdbcConnectionPoolMaxIdle =
        config.getUserLong(
            "storage_config",
            "jdbc_connection_pool_max_idle",
            (long) JdbcConfig.DEFAULT_CONNECTION_POOL_MAX_IDLE);
    long jdbcConnectionPoolMaxTotal =
        config.getUserLong(
            "storage_config",
            "jdbc_connection_pool_max_total",
            (long) JdbcConfig.DEFAULT_CONNECTION_POOL_MAX_TOTAL);
    boolean jdbcPreparedStatementsPoolEnabled =
        config.getUserBoolean(
            "storage_config",
            "jdbc_prepared_statements_pool_enabled",
            JdbcConfig.DEFAULT_PREPARED_STATEMENTS_POOL_ENABLED);
    long jdbcPreparedStatementsPoolMaxOpen =
        config.getUserLong(
            "storage_config",
            "jdbc_prepared_statements_pool_max_open",
            (long) JdbcConfig.DEFAULT_PREPARED_STATEMENTS_POOL_MAX_OPEN);
    long grpcDeadlineDurationMillis =
        config.getUserLong(
            "storage_config",
            "grpc_deadline_duration_millis",
            GrpcConfig.DEFAULT_DEADLINE_DURATION_MILLIS);

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoints);
    if (contactPort > 0) {
      props.setProperty(DatabaseConfig.CONTACT_PORT, Long.toString(contactPort));
    }
    props.setProperty(DatabaseConfig.USERNAME, username);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.STORAGE, storage);
    props.setProperty(DatabaseConfig.TRANSACTION_MANAGER, transactionManager);
    props.setProperty(DatabaseConfig.ISOLATION_LEVEL, isolationLevel);
    props.setProperty(ConsensusCommitConfig.SERIALIZABLE_STRATEGY, serializableStrategy);
    props.setProperty(
        JdbcConfig.CONNECTION_POOL_MIN_IDLE, Long.toString(jdbcConnectionPoolMinIdle));
    props.setProperty(
        JdbcConfig.CONNECTION_POOL_MAX_IDLE, Long.toString(jdbcConnectionPoolMaxIdle));
    props.setProperty(
        JdbcConfig.CONNECTION_POOL_MAX_TOTAL, Long.toString(jdbcConnectionPoolMaxTotal));
    props.setProperty(
        JdbcConfig.PREPARED_STATEMENTS_POOL_ENABLED,
        Boolean.toString(jdbcPreparedStatementsPoolEnabled));
    props.setProperty(
        JdbcConfig.PREPARED_STATEMENTS_POOL_MAX_OPEN,
        Long.toString(jdbcPreparedStatementsPoolMaxOpen));
    props.setProperty(
        GrpcConfig.DEADLINE_DURATION_MILLIS, Long.toString(grpcDeadlineDurationMillis));
    return new DatabaseConfig(props);
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
    } catch (CoordinatorException e) {
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
    IntervalFunction intervalFunc = IntervalFunction.ofExponentialBackoff(SLEEP_BASE_MILLIS, 2.0);

    RetryConfig retryConfig =
        RetryConfig.custom().maxAttempts(MAX_RETRIES).intervalFunction(intervalFunc).build();

    return Retry.of(name, retryConfig);
  }
}
