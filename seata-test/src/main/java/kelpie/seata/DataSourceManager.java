package kelpie.seata;

import com.alibaba.druid.pool.DruidDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import com.scalar.kelpie.config.Config;
import io.seata.rm.datasource.xa.DataSourceProxyXA;

public class DataSourceManager {
  private static final String DEFAULT_STORAGE_CONFIG_NAME = "db_config";
  private final Config config;
  private final DataSource dataSource;

  public DataSourceManager(Config config) {
    this.config = config;
    this.dataSource = getDataSourceXA(DEFAULT_STORAGE_CONFIG_NAME);
  }

  public DataSourceManager(Config config, String table) {
    this.config = config;
    this.dataSource = getDataSourceXA(table);
  }

  public Connection getConnectionXA() throws SQLException {
    return dataSource.getConnection();
  }

  private DataSource getDataSourceXA(String table) {
    String url = config.getUserString(table, "url", "jdbc:mysql://localhost/seata");
    String username = config.getUserString(table, "username", "cassandra");
    String password = config.getUserString(table, "password", "cassandra");
    String driver = config.getUserString(table, "isolation_level", "com.mysql.jdbc.Driver");

    DataSource dataSource = new DruidDataSource();
    ((DruidDataSource)dataSource).setUrl(url);
    ((DruidDataSource)dataSource).setUsername(username);
    ((DruidDataSource)dataSource).setPassword(password);
    ((DruidDataSource)dataSource).setDriverClassName(driver);
    return new DataSourceProxyXA(dataSource);
  }
}
