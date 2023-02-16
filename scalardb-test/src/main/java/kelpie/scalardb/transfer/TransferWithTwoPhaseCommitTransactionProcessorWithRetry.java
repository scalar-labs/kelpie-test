package kelpie.scalardb.transfer;

import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import javax.json.Json;

public class TransferWithTwoPhaseCommitTransactionProcessorWithRetry extends TimeBasedProcessor {

  private static final int INITIAL_INTERNAL_MILLS = 10;
  private static final double MULTIPLIER = 2.0;
  private static final int MAX_RETRIES = 10;

  private final TwoPhaseCommitTransactionManager manager1;
  private final TwoPhaseCommitTransactionManager manager2;
  private final int numAccounts;
  private final Retry retry;
  private final LongAdder retryCount = new LongAdder();

  public TransferWithTwoPhaseCommitTransactionProcessorWithRetry(Config config) {
    super(config);
    manager1 = TransferCommon.getTwoPhaseCommitTransactionManager1(config);
    manager2 = TransferCommon.getTwoPhaseCommitTransactionManager2(config);

    numAccounts = (int) config.getUserLong("test_config", "num_accounts");

    RetryConfig retryConfig =
        RetryConfig.custom()
            .maxAttempts(MAX_RETRIES)
            .intervalFunction(
                IntervalFunction.ofExponentialBackoff(INITIAL_INTERNAL_MILLS, MULTIPLIER))
            .retryExceptions(CrudConflictException.class, CommitConflictException.class)
            .build();
    retry = Retry.of("TransferWithTwoPhaseCommitTransactionProcessorWithRetry", retryConfig);
    retry
        .getEventPublisher()
        .onRetry(
            e -> {
              logWarn(e.toString(), e.getLastThrowable());
              retryCount.increment();
            });
  }

  @Override
  public void executeEach() throws Exception {
    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;

    try {
      Retry.decorateCheckedRunnable(retry, () -> transfer(fromId, toId, amount)).run();
    } catch (Throwable t) {
      if (t instanceof Exception) {
        throw (Exception) t;
      }

      throw new Exception(t);
    }
  }

  @Override
  public void close() {
    manager1.close();
    manager2.close();

    setState(Json.createObjectBuilder().add("retry_count", retryCount.longValue()).build());
  }

  private void transfer(int fromId, int toId, int amount) throws Exception {
    int fromType = 0;
    int toType = 0;
    if (fromId == toId) {
      toType = 1; // transfer between the same account
    }

    TwoPhaseCommitTransaction tx1 = manager1.start();
    TwoPhaseCommitTransaction tx2 = manager2.join(tx1.getId());
    try {
      Get fromGet = TransferCommon.prepareGet(fromId, fromType);
      Optional<Result> fromResult = tx1.get(fromGet);
      int fromBalance = TransferCommon.getBalanceFromResult(fromResult.get());
      Put fromPut = TransferCommon.preparePut(fromId, fromType, fromBalance - amount);
      tx1.put(fromPut);

      Get toGet = TransferCommon.prepareGet(toId, toType);
      Optional<Result> toResult = tx2.get(toGet);
      int toBalance = TransferCommon.getBalanceFromResult(toResult.get());
      Put toPut = TransferCommon.preparePut(toId, toType, toBalance + amount);
      tx2.put(toPut);

      tx1.prepare();
      tx2.prepare();
      tx1.validate();
      tx2.validate();
      tx1.commit();
      tx2.commit();
    } catch (Exception e) {
      try {
        tx1.rollback();
      } catch (RollbackException ex) {
        logWarn("rollback failed", ex);
      }
      try {
        tx2.rollback();
      } catch (RollbackException ex) {
        logWarn("rollback failed", ex);
      }
      throw e;
    }
  }
}
