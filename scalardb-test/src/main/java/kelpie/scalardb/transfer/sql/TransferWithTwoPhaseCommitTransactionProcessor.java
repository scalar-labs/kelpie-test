package kelpie.scalardb.transfer.sql;

import com.scalar.db.sql.Record;
import com.scalar.db.sql.ResultSet;
import com.scalar.db.sql.SqlSession;
import com.scalar.db.sql.SqlSessionFactory;
import com.scalar.db.sql.TransactionMode;
import com.scalar.db.sql.statement.BoundStatement;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import kelpie.scalardb.transfer.TransferCommon;

public class TransferWithTwoPhaseCommitTransactionProcessor extends TimeBasedProcessor {
  private final int numAccounts;
  private final SqlSessionFactory sqlSessionFactory1;
  private final SqlSessionFactory sqlSessionFactory2;

  public TransferWithTwoPhaseCommitTransactionProcessor(Config config) {
    super(config);
    numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    sqlSessionFactory1 =
        SqlCommon.getSqlSessionFactory1(config, TransactionMode.TWO_PHASE_COMMIT_TRANSACTION);
    sqlSessionFactory2 =
        SqlCommon.getSqlSessionFactory2(config, TransactionMode.TWO_PHASE_COMMIT_TRANSACTION);
  }

  @Override
  public void executeEach() throws Exception {
    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;

    try (SqlSession sqlSession1 = sqlSessionFactory1.createSqlSession();
        SqlSession sqlSession2 = sqlSessionFactory2.createSqlSession()) {
      transfer(sqlSession1, sqlSession2, fromId, toId, amount);
    }
  }

  @Override
  public void close() {
    try {
      sqlSessionFactory1.close();
    } catch (Exception e) {
      logWarn("Failed to close SqlSessionFactory", e);
    }

    try {
      sqlSessionFactory2.close();
    } catch (Exception e) {
      logWarn("Failed to close SqlSessionFactory", e);
    }
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
      String transactionId = sqlSession1.begin();
      sqlSession2.join(transactionId);

      BoundStatement boundStatement = sqlSession1.prepareStatement(SELECT_QUERY).bind();
      boundStatement.setInt(0, fromId);
      boundStatement.setInt(1, fromType);
      ResultSet resultSet = sqlSession1.execute(boundStatement);
      Optional<Record> record = resultSet.one();
      int fromBalance = record.get().getInt(TransferCommon.BALANCE);

      boundStatement = sqlSession1.prepareStatement(UPDATE_QUERY).bind();
      boundStatement.setInt(0, fromBalance - amount);
      boundStatement.setInt(1, fromId);
      boundStatement.setInt(2, fromType);
      sqlSession1.execute(boundStatement);

      boundStatement = sqlSession2.prepareStatement(SELECT_QUERY).bind();
      boundStatement.setInt(0, toId);
      boundStatement.setInt(1, toType);
      resultSet = sqlSession2.execute(boundStatement);
      record = resultSet.one();
      int toBalance = record.get().getInt(TransferCommon.BALANCE);

      boundStatement = sqlSession2.prepareStatement(UPDATE_QUERY).bind();
      boundStatement.setInt(0, toBalance + amount);
      boundStatement.setInt(1, toId);
      boundStatement.setInt(2, toType);
      sqlSession2.execute(boundStatement);

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
