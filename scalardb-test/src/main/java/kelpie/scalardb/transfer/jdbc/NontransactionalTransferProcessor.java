package kelpie.scalardb.transfer.jdbc;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclose;
import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngine;
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
  private final RdbEngine rdbEngine;

  public NontransactionalTransferProcessor(Config config) {
    super(config);
    this.numAccounts = (int) config.getUserLong("test_config", "num_accounts");

    DatabaseConfig databaseConfig = Common.getDatabaseConfig(config);
    dataSource = JdbcUtils.initDataSource(new JdbcConfig(databaseConfig.getProperties()));
    rdbEngine = JdbcUtils.getRdbEngine(databaseConfig.getContactPoints().get(0));
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
        enclosedFullTableName(TransferCommon.KEYSPACE, TransferCommon.TABLE, rdbEngine);
    final String enclosedAccountId = enclose(TransferCommon.ACCOUNT_ID, rdbEngine);
    final String enclosedAccountType = enclose(TransferCommon.ACCOUNT_TYPE, rdbEngine);
    final String enclosedBalance = enclose(TransferCommon.BALANCE, rdbEngine);

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
        enclosedFullTableName(TransferCommon.KEYSPACE, TransferCommon.TABLE, rdbEngine);
    final String enclosedAccountId = enclose(TransferCommon.ACCOUNT_ID, rdbEngine);
    final String enclosedAccountType = enclose(TransferCommon.ACCOUNT_TYPE, rdbEngine);
    final String enclosedBalance = enclose(TransferCommon.BALANCE, rdbEngine);

    String sql;
    switch (rdbEngine) {
      case MYSQL:
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
      case POSTGRESQL:
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
      case ORACLE:
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
      case SQL_SERVER:
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
      switch (rdbEngine) {
        case MYSQL:
        case POSTGRESQL:
          preparedStatement.setInt(1, id);
          preparedStatement.setInt(2, type);
          preparedStatement.setInt(3, amount);
          preparedStatement.setInt(4, amount);
          break;
        case ORACLE:
        case SQL_SERVER:
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
