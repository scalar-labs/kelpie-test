package kelpie.scalardb.transfer.sql;

import com.scalar.db.sql.PreparedStatement;
import com.scalar.db.sql.SqlSession;
import com.scalar.db.sql.SqlSessionFactory;
import com.scalar.db.sql.TransactionMode;
import com.scalar.db.sql.Value;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PreProcessor;
import io.github.resilience4j.retry.Retry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import kelpie.scalardb.Common;
import kelpie.scalardb.transfer.TransferCommon;

public class TransferPreparer extends PreProcessor {
  private static final long DEFAULT_POPULATION_CONCURRENCY = 32L;
  private static final long DEFAULT_POPULATION_NUM_ACCOUNTS_PER_TX = 100;
  private static final long DEFAULT_POPULATION_MAX_RETRIES = 10;
  private static final long DEFAULT_POPULATION_WAIT_MILLS = 1000;

  private final SqlSessionFactory sqlSessionFactory;

  public TransferPreparer(Config config) {
    super(config);
    sqlSessionFactory = SqlCommon.getSqlSessionFactory(config, TransactionMode.TRANSACTION);
  }

  @Override
  public void execute() {
    logInfo("insert initial values... ");

    int concurrency =
        (int)
            config.getUserLong(
                "test_config", "population_concurrency", DEFAULT_POPULATION_CONCURRENCY);
    ExecutorService es = Executors.newCachedThreadPool();
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    IntStream.range(0, concurrency)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(() -> new PopulationRunner(i).run(), es);
              futures.add(future);
            });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    logInfo("all records have been inserted");
  }

  @Override
  public void close() {
    sqlSessionFactory.close();
  }

  private class PopulationRunner {
    private final int id;

    public PopulationRunner(int threadId) {
      this.id = threadId;
    }

    public void run() {
      int concurrency =
          (int)
              config.getUserLong(
                  "test_config", "population_concurrency", DEFAULT_POPULATION_CONCURRENCY);
      int numAccountsPerTx =
          (int)
              config.getUserLong(
                  "test_config",
                  "population_num_accounts_per_tx",
                  DEFAULT_POPULATION_NUM_ACCOUNTS_PER_TX);
      int numAccounts = (int) config.getUserLong("test_config", "num_accounts");
      int numPerThread = (numAccounts + concurrency - 1) / concurrency;
      int start = numPerThread * id;
      int end = Math.min(numPerThread * (id + 1), numAccounts);
      IntStream.range(0, (numPerThread + numAccountsPerTx - 1) / numAccountsPerTx)
          .forEach(
              i -> {
                int startId = start + numAccountsPerTx * i;
                int endId = Math.min(start + numAccountsPerTx * (i + 1), end);
                populateWithTx(startId, endId);
              });
    }

    private void populateWithTx(int startId, int endId) {
      Runnable populate =
          () -> {
            try (SqlSession sqlSession = sqlSessionFactory.getSqlSession()) {
              try {
                sqlSession.begin();

                PreparedStatement preparedStatement =
                    sqlSession.prepareStatement(
                        "INSERT INTO "
                            + TransferCommon.KEYSPACE
                            + "."
                            + TransferCommon.TABLE
                            + " VALUES(?,?,?)");

                for (int i = startId; i < endId; ++i) {
                  for (int j = 0; j < TransferCommon.NUM_TYPES; ++j) {
                    preparedStatement.clearParameters();
                    preparedStatement.set(0, Value.ofInt(i));
                    preparedStatement.set(1, Value.ofInt(j));
                    preparedStatement.set(2, Value.ofInt(TransferCommon.INITIAL_BALANCE));
                    preparedStatement.execute();
                  }
                }

                sqlSession.commit();
              } catch (Exception e) {
                sqlSession.rollback();
                logWarn("population failed.", e);
                throw e;
              }
            }
          };

      int maxRetries =
          (int)
              config.getUserLong(
                  "test_config", "population_max_retries", DEFAULT_POPULATION_MAX_RETRIES);
      int waitMillis =
          (int)
              config.getUserLong(
                  "test_config", "population_wait_millis", DEFAULT_POPULATION_WAIT_MILLS);
      Retry retry = Common.getRetryWithFixedWaitDuration("populate", maxRetries, waitMillis);
      Runnable decorated = Retry.decorateRunnable(retry, populate);
      try {
        decorated.run();
      } catch (Exception e) {
        logError("population failed repeatedly!");
        throw e;
      }
    }
  }
}
