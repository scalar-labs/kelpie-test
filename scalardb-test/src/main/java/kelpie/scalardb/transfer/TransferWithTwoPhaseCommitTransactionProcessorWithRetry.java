package kelpie.scalardb.transfer;

import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.kelpie.config.Config;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.util.concurrent.atomic.LongAdder;
import javax.json.Json;

public class TransferWithTwoPhaseCommitTransactionProcessorWithRetry
    extends TransferWithTwoPhaseCommitTransactionProcessor {

  private static final int INITIAL_INTERNAL_MILLS = 10;
  private static final double MULTIPLIER = 2.0;
  private static final int MAX_RETRIES = 10;

  private final Retry retry;
  private final LongAdder retryCount = new LongAdder();

  public TransferWithTwoPhaseCommitTransactionProcessorWithRetry(Config config) {
    super(config);

    RetryConfig retryConfig =
        RetryConfig.custom()
            .maxAttempts(MAX_RETRIES)
            .intervalFunction(
                IntervalFunction.ofExponentialBackoff(INITIAL_INTERNAL_MILLS, MULTIPLIER))
            .retryExceptions(
                CrudConflictException.class,
                PreparationConflictException.class,
                ValidationConflictException.class,
                CommitConflictException.class)
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
    try {
      Retry.decorateCheckedRunnable(retry, super::executeEach).run();
    } catch (Throwable t) {
      if (t instanceof Exception) {
        throw (Exception) t;
      }

      throw new Exception(t);
    }
  }

  @Override
  public void close() {
    super.close();

    setState(Json.createObjectBuilder().add("retry_count", retryCount.longValue()).build());
  }
}
