package kelpie.scalardb.transfer.sql;

import com.scalar.db.sql.SqlSessionFactory;
import com.scalar.kelpie.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public final class SqlCommon {

  private SqlCommon() {}

  public static SqlSessionFactory getSqlSessionFactory(Config config) {
    String configFile = config.getUserString("sql_config", "config_file");
    return SqlSessionFactory.builder().withPropertiesFile(configFile).build();
  }

  public static HikariDataSource getDataSource(Config config) {
    String configFile = config.getUserString("sql_config", "config_file");

    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setDriverClassName("com.scalar.db.Driver");
    hikariConfig.setJdbcUrl("jdbc:scalardb:" + configFile);
    hikariConfig.setAutoCommit(false);
    hikariConfig.setMinimumIdle(
        (int) config.getUserLong("sql_config", "jdbc_connection_pool_min_idle", 20L));
    hikariConfig.setMaximumPoolSize(
        (int) config.getUserLong("sql_config", "jdbc_connection_pool_max_total", 200L));
    return new HikariDataSource(hikariConfig);
  }
}
