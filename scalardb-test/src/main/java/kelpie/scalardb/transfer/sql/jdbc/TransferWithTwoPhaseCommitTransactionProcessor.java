package kelpie.scalardb.transfer.sql.jdbc;

import com.scalar.db.sql.TransactionMode;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;
import kelpie.scalardb.transfer.TransferCommon;
import kelpie.scalardb.transfer.sql.SqlCommon;

public class TransferWithTwoPhaseCommitTransactionProcessor extends TimeBasedProcessor {
  private final int numAccounts;
  private final HikariDataSource dataSource1;
  private final HikariDataSource dataSource2;

  public TransferWithTwoPhaseCommitTransactionProcessor(Config config) {
    super(config);
    numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    dataSource1 = SqlCommon.getDataSource1(config, TransactionMode.TWO_PHASE_COMMIT_TRANSACTION);
    dataSource2 = SqlCommon.getDataSource2(config, TransactionMode.TWO_PHASE_COMMIT_TRANSACTION);
  }

  @Override
  public void executeEach() throws Exception {
    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;

    try (Connection connection1 = dataSource1.getConnection();
        Connection connection2 = dataSource2.getConnection()) {
      transfer(connection1, connection2, fromId, toId, amount);
    }
  }

  @Override
  public void close() {
    dataSource1.close();
    dataSource2.close();
  }

  private void transfer(
      Connection connection1, Connection connection2, int fromId, int toId, int amount)
      throws SQLException {
    int fromType = 0;
    int toType = 0;
    if (fromId == toId) {
      toType = 1; // transfer between the same account
    }

    final String SELECT_QUERY =
        "SELECT * FROM "
            + TransferCommon.NAMESPACE
            + "."
            + TransferCommon.TABLE
            + " WHERE "
            + TransferCommon.ACCOUNT_ID
            + "=? AND "
            + TransferCommon.ACCOUNT_TYPE
            + "=?";

    final String UPDATE_QUERY =
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
            + "=?";

    try {
      String transactionId;
      try (Statement statement = connection1.createStatement()) {
        statement.execute("BEGIN");
        java.sql.ResultSet resultSet = statement.getResultSet();
        resultSet.next();
        transactionId = resultSet.getString(1);
      }

      try (Statement statement = connection2.createStatement()) {
        statement.execute("JOIN '" + transactionId + "'");
      }

      try (PreparedStatement preparedStatementForSelect =
              connection1.prepareStatement(SELECT_QUERY);
          PreparedStatement preparedStatementForUpdate =
              connection1.prepareStatement(UPDATE_QUERY)) {

        preparedStatementForSelect.setInt(1, fromId);
        preparedStatementForSelect.setInt(2, fromType);

        int fromBalance;
        try (ResultSet resultSet = preparedStatementForSelect.executeQuery()) {
          resultSet.next();
          fromBalance = resultSet.getInt(TransferCommon.BALANCE);
        }

        preparedStatementForUpdate.setInt(1, fromBalance - amount);
        preparedStatementForUpdate.setInt(2, fromId);
        preparedStatementForUpdate.setInt(3, fromType);
        preparedStatementForUpdate.execute();
      }

      try (PreparedStatement preparedStatementForSelect =
              connection2.prepareStatement(SELECT_QUERY);
          PreparedStatement preparedStatementForUpdate =
              connection2.prepareStatement(UPDATE_QUERY)) {

        preparedStatementForSelect.setInt(1, toId);
        preparedStatementForSelect.setInt(2, toType);

        int toBalance;
        try (ResultSet resultSet = preparedStatementForSelect.executeQuery()) {
          resultSet.next();
          toBalance = resultSet.getInt(TransferCommon.BALANCE);
        }

        preparedStatementForUpdate.setInt(1, toBalance + amount);
        preparedStatementForUpdate.setInt(2, toId);
        preparedStatementForUpdate.setInt(3, toType);
        preparedStatementForUpdate.execute();
      }

      try (Statement statement1 = connection1.createStatement();
          Statement statement2 = connection2.createStatement()) {
        statement1.execute("PREPARE");
        statement2.execute("PREPARE");
        statement1.execute("VALIDATE");
        statement2.execute("VALIDATE");
      }

      connection1.commit();
      connection2.commit();
    } catch (SQLException e) {
      connection1.rollback();
      connection2.rollback();
      throw e;
    }
  }
}
