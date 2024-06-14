package kelpie.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import com.scalar.kelpie.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbcp2.BasicDataSource;

public class DataSourceManager {
  private static final String DEFAULT_DB_CONFIG_NAME = "db_config";
  private final Config config;
  private final DataSource dataSource;

  public DataSourceManager(Config config) {
    this.config = config;
    this.dataSource = getDataSourceDbcp(DEFAULT_DB_CONFIG_NAME);
  }

  public DataSourceManager(Config config, String table) {
    this.config = config;
    this.dataSource = getDataSourceDbcp(table);
  }

  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  private DataSource getDataSourceDbcp(String table) {
    String url = config.getUserString(table, "url", "jdbc:mysql://localhost/jdbc_test");
    String driver = config.getUserString(table, "driver", "com.mysql.cj.jdbc.Driver");
    String username = config.getUserString(table, "username", "root");
    String password = config.getUserString(table, "password", "example");
    int minIdle = (int)config.getUserLong(table, "min_idle", (long)0);
    int maxActive = (int)config.getUserLong(table, "max_active", (long)8);

    BasicDataSource dataSource = new BasicDataSource();

    dataSource.setUrl(url);
    dataSource.setDriverClassName(driver);
    dataSource.setUsername(username);
    dataSource.setPassword(password);
    dataSource.setMinIdle(minIdle);
    dataSource.setMaxTotal(maxActive);
    return (DataSource)dataSource;
  }

  private DataSource getDataSourceHikari(String table) {
    String url = config.getUserString(table, "url", "jdbc:mysql://localhost/jdbc_test");
    String driver = config.getUserString(table, "driver", "com.mysql.cj.jdbc.Driver");
    String username = config.getUserString(table, "username", "root");
    String password = config.getUserString(table, "password", "example");
    int minIdle = (int)config.getUserLong(table, "min_idle", (long)0);
    int maxActive = (int)config.getUserLong(table, "max_active", (long)8);

    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(url);
    config.setDriverClassName(driver);
    config.addDataSourceProperty("user", username);
    config.addDataSourceProperty("password", password);
    config.addDataSourceProperty("minimumIdle", minIdle);
    config.addDataSourceProperty("maximumPoolSize", maxActive);

    return new HikariDataSource(config);
  }
}
