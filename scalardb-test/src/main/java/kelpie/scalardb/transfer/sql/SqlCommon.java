package kelpie.scalardb.transfer.sql;

import com.scalar.db.sql.SqlConfig;
import com.scalar.db.sql.SqlSessionFactory;
import com.scalar.db.sql.TransactionMode;
import com.scalar.kelpie.config.Config;
import org.apache.commons.dbcp2.BasicDataSource;

public final class SqlCommon {

  private SqlCommon() {}

  public static SqlSessionFactory getSqlSessionFactory(
      Config config, TransactionMode transactionMode) {
    String configFile = config.getUserString("sql_config", "config_file");
    return SqlSessionFactory.builder()
        .withPropertyFile(configFile)
        .withDefaultTransactionMode(transactionMode)
        .build();
  }

  public static BasicDataSource getDataSource(Config config, TransactionMode transactionMode) {
    String configFile = config.getUserString("sql_config", "config_file");

    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setDriver(new com.scalar.db.Driver());
    dataSource.setUrl(
        "jdbc:scalardb:"
            + configFile
            + "?"
            + SqlConfig.DEFAULT_TRANSACTION_MODE
            + "="
            + transactionMode.name());
    dataSource.setDefaultAutoCommit(false);
    dataSource.setAutoCommitOnReturn(false);
    dataSource.setMinIdle(
        (int) config.getUserLong("sql_config", "jdbc_connection_pool_min_idle", 20L));
    dataSource.setMaxIdle(
        (int) config.getUserLong("sql_config", "jdbc_connection_pool_max_idle", 50L));
    dataSource.setMaxTotal(
        (int) config.getUserLong("sql_config", "jdbc_connection_pool_max_total", 200L));
    return dataSource;
  }
}
