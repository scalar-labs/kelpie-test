package kelpie.scalardb.transfer.sql;

import com.scalar.db.sql.PreparedStatement;
import com.scalar.db.sql.Record;
import com.scalar.db.sql.ResultSet;
import com.scalar.db.sql.SqlSession;
import com.scalar.db.sql.SqlSessionFactory;
import com.scalar.db.sql.TransactionMode;
import com.scalar.db.sql.Value;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import kelpie.scalardb.transfer.TransferCommon;

public class TransferProcessor extends TimeBasedProcessor {
  private final int numAccounts;
  private final SqlSessionFactory sqlSessionFactory;

  public TransferProcessor(Config config) {
    super(config);
    numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    sqlSessionFactory = SqlCommon.getSqlSessionFactory(config, TransactionMode.TRANSACTION);
  }

  @Override
  public void executeEach() throws Exception {
    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;

    try (SqlSession sqlSession = sqlSessionFactory.createSqlSession()) {
      transfer(sqlSession, fromId, toId, amount);
    }
  }

  @Override
  public void close() {
    try {
      sqlSessionFactory.close();
    } catch (Exception e) {
      logWarn("Failed to close SqlSessionFactory", e);
    }
  }

  private void transfer(SqlSession sqlSession, int fromId, int toId, int amount) {
    int fromType = 0;
    int toType = 0;
    if (fromId == toId) {
      toType = 1; // transfer between the same account
    }

    try {
      sqlSession.begin();

      PreparedStatement preparedStatement;
      ResultSet resultSet;
      Optional<Record> record;

      preparedStatement =
          sqlSession.prepareStatement(
              "SELECT * FROM "
                  + TransferCommon.NAMESPACE
                  + "."
                  + TransferCommon.TABLE
                  + " WHERE "
                  + TransferCommon.ACCOUNT_ID
                  + "=? AND "
                  + TransferCommon.ACCOUNT_TYPE
                  + "=?");

      preparedStatement.set(0, Value.ofInt(fromId));
      preparedStatement.set(1, Value.ofInt(fromType));
      resultSet = preparedStatement.execute();
      record = resultSet.one();
      int fromBalance = record.get().getInt(TransferCommon.BALANCE);

      preparedStatement.clearParameters();
      preparedStatement.set(0, Value.ofInt(toId));
      preparedStatement.set(1, Value.ofInt(toType));
      resultSet = preparedStatement.execute();
      record = resultSet.one();
      int toBalance = record.get().getInt(TransferCommon.BALANCE);

      preparedStatement =
          sqlSession.prepareStatement(
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
                  + "=?");

      preparedStatement.set(0, Value.ofInt(fromBalance - amount));
      preparedStatement.set(1, Value.ofInt(fromId));
      preparedStatement.set(2, Value.ofInt(fromType));
      preparedStatement.execute();

      preparedStatement.clearParameters();
      preparedStatement.set(0, Value.ofInt(toBalance + amount));
      preparedStatement.set(1, Value.ofInt(toId));
      preparedStatement.set(2, Value.ofInt(toType));
      preparedStatement.execute();

      sqlSession.commit();
    } catch (Exception e) {
      sqlSession.rollback();
      throw e;
    }
  }
}
