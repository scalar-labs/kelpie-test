package kelpie.scalardb.transfer;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcDatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import kelpie.scalardb.Common;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;

public class TransferWithRawSqlProcessor extends TimeBasedProcessor {
  private final int numAccounts;
  private final BasicDataSource dataSource;
  private final RdbEngine rdbEngine;

  public TransferWithRawSqlProcessor(Config config) {
    super(config);
    this.numAccounts = (int) config.getUserLong("test_config", "num_accounts");

    DatabaseConfig databaseConfig = Common.getDatabaseConfig(config);
    dataSource = JdbcUtils.initDataSource(new JdbcDatabaseConfig(databaseConfig.getProperties()));
    rdbEngine = JdbcUtils.getRdbEngine(databaseConfig.getContactPoints().get(0));
    if (rdbEngine != RdbEngine.MYSQL && rdbEngine != RdbEngine.POSTGRESQL) {
      throw new IllegalArgumentException(rdbEngine + " is not supported.");
    }
  }

  @Override
  public void executeEach() throws Exception {
    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;

    try {
      transfer(fromId, toId, amount);
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public void close() {
    try {
      dataSource.close();
    } catch (SQLException e) {
      throw new RuntimeException("fail to close the dataSource", e);
    }
  }

  private void transfer(int fromId, int toId, int amount) throws SQLException {
    int fromType = 0;
    int toType = 0;
    if (fromId == toId) {
      toType = 1; // transfer between the same account
    }

    int fromBalance = get(fromId, fromType);
    int toBalance = get(toId, toType);

    put(fromId, fromType, fromBalance - amount);
    put(toId, toType, toBalance + amount);
  }

  private int get(int id, int type) throws SQLException {
    String sql =
        "SELECT "
            + TransferCommon.BALANCE
            + " FROM "
            + TransferCommon.KEYSPACE
            + "."
            + TransferCommon.TABLE
            + " WHERE "
            + TransferCommon.ACCOUNT_ID
            + " = "
            + id
            + " AND "
            + TransferCommon.ACCOUNT_TYPE
            + " = "
            + type;

    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      resultSet.next();
      return resultSet.getInt(TransferCommon.BALANCE);
    }
  }

  private void put(int id, int type, int amount) throws SQLException {
    String sql;
    switch (rdbEngine) {
      case MYSQL:
        sql =
            "INSERT INTO "
                + TransferCommon.KEYSPACE
                + "."
                + TransferCommon.TABLE
                + " ("
                + TransferCommon.ACCOUNT_ID
                + ","
                + TransferCommon.ACCOUNT_TYPE
                + ","
                + TransferCommon.BALANCE
                + ") VALUES ("
                + id
                + ","
                + type
                + ","
                + amount
                + ") ON DUPLICATE KEY UPDATE "
                + TransferCommon.BALANCE
                + "="
                + amount;
        break;
      case POSTGRESQL:
        sql =
            "INSERT INTO "
                + TransferCommon.KEYSPACE
                + "."
                + TransferCommon.TABLE
                + " ("
                + TransferCommon.ACCOUNT_ID
                + ","
                + TransferCommon.ACCOUNT_TYPE
                + ","
                + TransferCommon.BALANCE
                + ") VALUES ("
                + id
                + ","
                + type
                + ","
                + amount
                + ") ON CONFLICT ("
                + TransferCommon.ACCOUNT_ID
                + ","
                + TransferCommon.ACCOUNT_TYPE
                + ") DO UPDATE SET "
                + TransferCommon.BALANCE
                + "="
                + amount;
        break;
      default:
        throw new AssertionError();
    }

    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeUpdate(sql);
    }
  }
}
