package kelpie.scalardb.transfer.sql;

import com.scalar.db.sql.SqlConfig;
import com.scalar.db.sql.SqlSessionFactory;
import com.scalar.db.sql.TransactionMode;
import com.scalar.kelpie.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public final class SqlCommon {

  private SqlCommon() {}

  public static SqlSessionFactory getSqlSessionFactory(
      Config config, TransactionMode transactionMode) {
    return getSqlSessionFactory(config, transactionMode, "config_file");
  }

  public static SqlSessionFactory getSqlSessionFactory1(
      Config config, TransactionMode transactionMode) {
    return getSqlSessionFactory(config, transactionMode, "config_file1");
  }

  public static SqlSessionFactory getSqlSessionFactory2(
      Config config, TransactionMode transactionMode) {
    return getSqlSessionFactory(config, transactionMode, "config_file2");
  }

  private static SqlSessionFactory getSqlSessionFactory(
      Config config, TransactionMode transactionMode, String configName) {
    String configFile = config.getUserString("sql_config", configName);
    return SqlSessionFactory.builder()
        .withPropertiesFile(configFile)
        .withDefaultTransactionMode(transactionMode)
        .build();
  }

  public static HikariDataSource getDataSource(Config config, TransactionMode transactionMode) {
    return getDataSource(config, transactionMode, "config_file");
  }

  public static HikariDataSource getDataSource1(Config config, TransactionMode transactionMode) {
    return getDataSource(config, transactionMode, "config_file1");
  }

  public static HikariDataSource getDataSource2(Config config, TransactionMode transactionMode) {
    return getDataSource(config, transactionMode, "config_file2");
  }

  private static HikariDataSource getDataSource(
      Config config, TransactionMode transactionMode, String configName) {
    String configFile = config.getUserString("sql_config", configName);

    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setDriverClassName("com.scalar.db.Driver");
    hikariConfig.setJdbcUrl(
        "jdbc:scalardb:"
            + configFile
            + "?"
            + SqlConfig.DEFAULT_TRANSACTION_MODE
            + "="
            + transactionMode.name());
    hikariConfig.setAutoCommit(false);
    hikariConfig.setMinimumIdle(
        (int) config.getUserLong("sql_config", "jdbc_connection_pool_min_idle", 20L));
    hikariConfig.setMaximumPoolSize(
        (int) config.getUserLong("sql_config", "jdbc_connection_pool_max_total", 200L));
    return new HikariDataSource(hikariConfig);
  }
}
