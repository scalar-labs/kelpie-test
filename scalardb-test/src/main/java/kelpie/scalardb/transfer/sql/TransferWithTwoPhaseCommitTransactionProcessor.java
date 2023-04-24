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

public class TransferWithTwoPhaseCommitTransactionProcessor extends TimeBasedProcessor {
  private final int numAccounts;
  private final SqlSessionFactory sqlSessionFactory;

  public TransferWithTwoPhaseCommitTransactionProcessor(Config config) {
    super(config);
    numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    sqlSessionFactory =
        SqlCommon.getSqlSessionFactory(config, TransactionMode.TWO_PHASE_COMMIT_TRANSACTION);
  }

  @Override
  public void executeEach() throws Exception {
    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;

    try (SqlSession sqlSession1 = sqlSessionFactory.createSqlSession();
        SqlSession sqlSession2 = sqlSessionFactory.createSqlSession()) {
      transfer(sqlSession1, sqlSession2, fromId, toId, amount);
    }
  }

  @Override
  public void close() {
    sqlSessionFactory.close();
  }

  private void transfer(
      SqlSession sqlSession1, SqlSession sqlSession2, int fromId, int toId, int amount) {
    int fromType = 0;
    int toType = 0;
    if (fromId == toId) {
      toType = 1; // transfer between the same account
    }

    final String SELECT_QUERY =
        "SELECT * FROM "
            + TransferCommon.KEYSPACE
            + "."
            + TransferCommon.TABLE
            + " WHERE "
            + TransferCommon.ACCOUNT_ID
            + "=? AND "
            + TransferCommon.ACCOUNT_TYPE
            + "=?";

    final String UPDATE_QUERY =
        "UPDATE "
            + TransferCommon.KEYSPACE
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
      String transactionId = sqlSession1.begin();
      sqlSession2.join(transactionId);

      PreparedStatement preparedStatement = sqlSession1.prepareStatement(SELECT_QUERY);
      preparedStatement.set(0, Value.ofInt(fromId));
      preparedStatement.set(1, Value.ofInt(fromType));
      ResultSet resultSet = preparedStatement.execute();
      Optional<Record> record = resultSet.one();
      int fromBalance = record.get().getInt(TransferCommon.BALANCE);

      preparedStatement = sqlSession1.prepareStatement(UPDATE_QUERY);
      preparedStatement.set(0, Value.ofInt(fromBalance - amount));
      preparedStatement.set(1, Value.ofInt(fromId));
      preparedStatement.set(2, Value.ofInt(fromType));
      preparedStatement.execute();

      preparedStatement = sqlSession2.prepareStatement(SELECT_QUERY);
      preparedStatement.set(0, Value.ofInt(toId));
      preparedStatement.set(1, Value.ofInt(toType));
      resultSet = preparedStatement.execute();
      record = resultSet.one();
      int toBalance = record.get().getInt(TransferCommon.BALANCE);

      preparedStatement = sqlSession2.prepareStatement(UPDATE_QUERY);
      preparedStatement.set(0, Value.ofInt(toBalance + amount));
      preparedStatement.set(1, Value.ofInt(toId));
      preparedStatement.set(2, Value.ofInt(toType));
      preparedStatement.execute();

      sqlSession1.prepare();
      sqlSession2.prepare();
      sqlSession1.validate();
      sqlSession2.validate();
      sqlSession1.commit();
      sqlSession2.commit();
    } catch (Exception e) {
      sqlSession1.rollback();
      sqlSession2.rollback();
      throw e;
    }
  }
}
