package kelpie.scalardb.transfer;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
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

public class TransferProcessorWithRetry extends TimeBasedProcessor {

  private static final int INITIAL_INTERNAL_MILLS = 10;
  private static final double MULTIPLIER = 2.0;
  private static final int MAX_RETRIES = 10;

  private final DistributedTransactionManager manager;
  private final int numAccounts;
  private final Retry retry;
  private final LongAdder retryCount = new LongAdder();

  public TransferProcessorWithRetry(Config config) {
    super(config);
    this.manager = TransferCommon.getTransactionManager(config);
    this.numAccounts = (int) config.getUserLong("test_config", "num_accounts");

    RetryConfig retryConfig =
        RetryConfig.custom()
            .maxAttempts(MAX_RETRIES)
            .intervalFunction(
                IntervalFunction.ofExponentialBackoff(INITIAL_INTERNAL_MILLS, MULTIPLIER))
            .retryExceptions(CrudConflictException.class, CommitConflictException.class)
            .build();
    retry = Retry.of("TransferProcessorWithRetry", retryConfig);
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
    manager.close();

    setState(Json.createObjectBuilder().add("retry_count", retryCount.longValue()).build());
  }

  private void transfer(int fromId, int toId, int amount) throws Exception {
    int fromType = 0;
    int toType = 0;
    if (fromId == toId) {
      toType = 1; // transfer between the same account
    }

    DistributedTransaction transaction = manager.begin();
    try {
      Get fromGet = TransferCommon.prepareGet(fromId, fromType);
      Get toGet = TransferCommon.prepareGet(toId, toType);

      Optional<Result> fromResult = transaction.get(fromGet);
      Optional<Result> toResult = transaction.get(toGet);
      int fromBalance = TransferCommon.getBalanceFromResult(fromResult.get());
      int toBalance = TransferCommon.getBalanceFromResult(toResult.get());

      Put fromPut = TransferCommon.preparePut(fromId, fromType, fromBalance - amount);
      Put toPut = TransferCommon.preparePut(toId, toType, toBalance + amount);
      transaction.put(fromPut);
      transaction.put(toPut);

      transaction.commit();
    } catch (Exception e) {
      if (e instanceof CrudConflictException || e instanceof CommitConflictException) {
        logWarn("a transaction conflict occurred. Retrying...", e);
        retryCount.increment();
      }

      try {
        transaction.rollback();
      } catch (RollbackException ex) {
        logWarn("rollback failed", ex);
      }
      throw e;
    }
  }
}
