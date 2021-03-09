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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

public class SqlBasedNontransactionalTransferProcessor extends TimeBasedProcessor {
  private final int numAccounts;
  private final BasicDataSource dataSource;
  private final RdbEngine rdbEngine;

  public SqlBasedNontransactionalTransferProcessor(Config config) {
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

    transfer(fromId, toId, amount);
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
            + "=? AND "
            + TransferCommon.ACCOUNT_TYPE
            + "=?";

    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      preparedStatement.setInt(1, id);
      preparedStatement.setInt(2, type);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        resultSet.next();
        return resultSet.getInt(TransferCommon.BALANCE);
      }
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
                + ") VALUES (?,?,?) ON DUPLICATE KEY UPDATE "
                + TransferCommon.BALANCE
                + "=?";
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
                + ") VALUES (?,?,?) ON CONFLICT ("
                + TransferCommon.ACCOUNT_ID
                + ","
                + TransferCommon.ACCOUNT_TYPE
                + ") DO UPDATE SET "
                + TransferCommon.BALANCE
                + "=?";
        break;
      default:
        throw new AssertionError();
    }

    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      preparedStatement.setInt(1, id);
      preparedStatement.setInt(2, type);
      preparedStatement.setInt(3, amount);
      preparedStatement.setInt(4, amount);
      preparedStatement.executeUpdate();
    }
  }
}
