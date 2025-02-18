package kelpie.scalardb.transfer.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngineFactory;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import kelpie.scalardb.Common;
import kelpie.scalardb.transfer.TransferCommon;
import org.apache.commons.dbcp2.BasicDataSource;

public class NontransactionalTransferProcessor extends TimeBasedProcessor {
  private final int numAccounts;
  private final BasicDataSource dataSource;
  private final RdbEngineStrategy rdbEngine;

  public NontransactionalTransferProcessor(Config config) {
    super(config);
    this.numAccounts = (int) config.getUserLong("test_config", "num_accounts");

    DatabaseConfig databaseConfig = Common.getDatabaseConfig(config);
    JdbcConfig jdbcConfig = new JdbcConfig(databaseConfig);
    rdbEngine = RdbEngineFactory.create(jdbcConfig);
    dataSource = JdbcUtils.initDataSource(jdbcConfig, rdbEngine);
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
    final String enclosedFullTableName =
        rdbEngine.encloseFullTableName(TransferCommon.NAMESPACE, TransferCommon.TABLE);
    final String enclosedAccountId = rdbEngine.enclose(TransferCommon.ACCOUNT_ID);
    final String enclosedAccountType = rdbEngine.enclose(TransferCommon.ACCOUNT_TYPE);
    final String enclosedBalance = rdbEngine.enclose(TransferCommon.BALANCE);

    String sql =
        "SELECT "
            + enclosedBalance
            + " FROM "
            + enclosedFullTableName
            + " WHERE "
            + enclosedAccountId
            + "=? AND "
            + enclosedAccountType
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
    final String enclosedFullTableName =
        rdbEngine.encloseFullTableName(TransferCommon.NAMESPACE, TransferCommon.TABLE);
    final String enclosedAccountId = rdbEngine.enclose(TransferCommon.ACCOUNT_ID);
    final String enclosedAccountType = rdbEngine.enclose(TransferCommon.ACCOUNT_TYPE);
    final String enclosedBalance = rdbEngine.enclose(TransferCommon.BALANCE);

    String sql;
    switch (rdbEngine.getClass().getSimpleName()) {
      case "RdbEngineMysql":
        sql =
            "INSERT INTO "
                + enclosedFullTableName
                + " ("
                + enclosedAccountId
                + ","
                + enclosedAccountType
                + ","
                + enclosedBalance
                + ") VALUES (?,?,?) ON DUPLICATE KEY UPDATE "
                + enclosedBalance
                + "=?";
        break;
      case "RdbEnginePostgresql":
        sql =
            "INSERT INTO "
                + enclosedFullTableName
                + " ("
                + enclosedAccountId
                + ","
                + enclosedAccountType
                + ","
                + enclosedBalance
                + ") VALUES (?,?,?) ON CONFLICT ("
                + enclosedAccountId
                + ","
                + enclosedAccountType
                + ") DO UPDATE SET "
                + enclosedBalance
                + "=?";
        break;
      case "RdbEngineOracle":
        sql =
            "MERGE INTO "
                + enclosedFullTableName
                + "t1 USING (SELECT ? "
                + enclosedAccountId
                + ",? "
                + enclosedAccountType
                + " FROM DUAL) t2 ON (t1."
                + enclosedAccountId
                + "=t2."
                + enclosedAccountId
                + " AND t1."
                + enclosedAccountType
                + "=t2."
                + enclosedAccountType
                + ") WHEN MATCHED THEN UPDATE SET "
                + enclosedBalance
                + "=? WHEN NOT MATCHED THEN INSERT ("
                + enclosedAccountId
                + ","
                + enclosedAccountType
                + ","
                + enclosedBalance
                + ") VALUES (?,?,?)";
        break;
      case "RdbEngineSqlServer":
        sql =
            "MERGE "
                + enclosedFullTableName
                + " t1 USING (SELECT ? "
                + enclosedAccountId
                + ",? "
                + enclosedAccountType
                + ") t2 "
                + "ON (t1."
                + enclosedAccountId
                + "=t2."
                + enclosedAccountId
                + " AND t1."
                + enclosedAccountType
                + "=t2."
                + enclosedAccountType
                + ") WHEN MATCHED THEN UPDATE SET "
                + enclosedBalance
                + "=? WHEN NOT MATCHED THEN INSERT ("
                + enclosedAccountId
                + ","
                + enclosedAccountType
                + ","
                + enclosedBalance
                + ") VALUES (?,?,?);";
        break;
      default:
        throw new AssertionError();
    }

    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      switch (rdbEngine.getClass().getSimpleName()) {
        case "RdbEngineMysql":
        case "RdbEnginePostgresql":
          preparedStatement.setInt(1, id);
          preparedStatement.setInt(2, type);
          preparedStatement.setInt(3, amount);
          preparedStatement.setInt(4, amount);
          break;
        case "RdbEngineOracle":
        case "RdbEngineSqlServer":
          preparedStatement.setInt(1, id);
          preparedStatement.setInt(2, type);
          preparedStatement.setInt(3, amount);
          preparedStatement.setInt(4, id);
          preparedStatement.setInt(5, type);
          preparedStatement.setInt(6, amount);
          break;
        default:
          throw new AssertionError();
      }
      preparedStatement.executeUpdate();
    }
  }
}
