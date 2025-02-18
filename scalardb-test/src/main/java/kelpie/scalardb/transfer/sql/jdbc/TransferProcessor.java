package kelpie.scalardb.transfer.sql.jdbc;

import com.scalar.db.sql.TransactionMode;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import kelpie.scalardb.transfer.TransferCommon;
import kelpie.scalardb.transfer.sql.SqlCommon;
import org.apache.commons.dbcp2.BasicDataSource;

public class TransferProcessor extends TimeBasedProcessor {
  private final int numAccounts;
  private final BasicDataSource dataSource;

  public TransferProcessor(Config config) {
    super(config);
    numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    dataSource = SqlCommon.getDataSource(config, TransactionMode.TRANSACTION);
  }

  @Override
  public void executeEach() throws Exception {
    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;

    try (Connection connection = dataSource.getConnection()) {
      transfer(connection, fromId, toId, amount);
    }
  }

  @Override
  public void close() {
    try {
      dataSource.close();
    } catch (SQLException e) {
      logWarn("Failed to close DataSource", e);
    }
  }

  private void transfer(Connection connection, int fromId, int toId, int amount)
      throws SQLException {
    int fromType = 0;
    int toType = 0;
    if (fromId == toId) {
      toType = 1; // transfer between the same account
    }

    int fromBalance;
    int toBalance;

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(
            "SELECT * FROM "
                + TransferCommon.NAMESPACE
                + "."
                + TransferCommon.TABLE
                + " WHERE "
                + TransferCommon.ACCOUNT_ID
                + "=? AND "
                + TransferCommon.ACCOUNT_TYPE
                + "=?")) {
      preparedStatement.setInt(1, fromId);
      preparedStatement.setInt(2, fromType);

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        resultSet.next();
        fromBalance = resultSet.getInt(TransferCommon.BALANCE);
      }

      preparedStatement.clearParameters();
      preparedStatement.setInt(1, toId);
      preparedStatement.setInt(2, toType);

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        resultSet.next();
        toBalance = resultSet.getInt(TransferCommon.BALANCE);
      }
    } catch (SQLException e) {
      connection.rollback();
      throw e;
    }

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(
            "UPDATE "
                + TransferCommon.NAMESPACE
                + "."
                + TransferCommon.TABLE
                + " SET "
                + TransferCommon.BALANCE
                + "=?"
                + " WHERE "
                + TransferCommon.ACCOUNT_ID
                + "=? AND "
                + TransferCommon.ACCOUNT_TYPE
                + "=?")) {

      preparedStatement.setInt(1, fromBalance - amount);
      preparedStatement.setInt(2, fromId);
      preparedStatement.setInt(3, fromType);
      preparedStatement.execute();

      preparedStatement.clearParameters();
      preparedStatement.setInt(1, toBalance + amount);
      preparedStatement.setInt(2, toId);
      preparedStatement.setInt(3, toType);
      preparedStatement.execute();
    } catch (SQLException e) {
      connection.rollback();
      throw e;
    }

    connection.commit();
  }
}
