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

public class TransferReadOnlyProcessor extends TimeBasedProcessor {
  private final int numAccounts;
  private final SqlSessionFactory sqlSessionFactory;

  public TransferReadOnlyProcessor(Config config) {
    super(config);
    numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    sqlSessionFactory = SqlCommon.getSqlSessionFactory(config, TransactionMode.TRANSACTION);
  }

  @Override
  public void executeEach() throws Exception {
    int id = ThreadLocalRandom.current().nextInt(numAccounts);

    try (SqlSession sqlSession = sqlSessionFactory.createSqlSession()) {
      PreparedStatement preparedStatement =
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
      preparedStatement.set(0, Value.ofInt(id));
      preparedStatement.set(1, Value.ofInt(0));
      ResultSet resultSet = preparedStatement.execute();
      Optional<Record> record = resultSet.one();
      if (!record.isPresent()) {
        logWarn("The record doesn't exist. ID: " + id);
      }
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
}
