package kelpie.scalardb.sensor;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.exception.PostProcessException;
import com.scalar.kelpie.modules.PostProcessor;
import io.github.resilience4j.retry.Retry;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import kelpie.scalardb.Common;

public class SensorChecker extends PostProcessor {
  private final DistributedTransactionManager manager;

  public SensorChecker(Config config) {
    super(config);
    this.manager = Common.getTransactionManager(config);
  }

  @Override
  public void execute() {
    int startTimestamp = getIntFromPreviousState("start_timestamp");
    int endTimestamp = getIntFromPreviousState("end_timestamp");

    boolean isDuplicated = false;
    for (int i = startTimestamp; i <= endTimestamp; i++) {
      List<Result> results = readRecordsWithRetry(i);

      boolean hasDuplicatedRevision = SensorCommon.hasDuplicatedRevision(results);
      if (hasDuplicatedRevision) {
        isDuplicated = true;
        logError("There is a duplicated revision at " + i);
      }
    }

    if (isDuplicated) {
      logError("Duplication happened !");
      throw new PostProcessException("Inconsistency happened!");
    }
  }

  @Override
  public void close() {
    try {
      manager.close();
    } catch (Exception e) {
      logWarn("Failed to close the transaction manager", e);
    }
  }

  private List<Result> readRecordsWithRetry(int timestamp) {
    int maxRetry = (int) config.getUserLong("test_config", "checker_max_retries_for_read", 10L);
    long retryIntervalSleepTime =
        config.getUserLong("test_config", "checker_retry_interval_millis", 1000L);
    Retry retry =
        Common.getRetryWithExponentialBackoff("readBalances", maxRetry, retryIntervalSleepTime);
    Supplier<List<Result>> decorated = Retry.decorateSupplier(retry, () -> readRecords(timestamp));

    try {
      return decorated.get();
    } catch (Exception e) {
      throw new RuntimeException("Reading records failed repeatedly", e);
    }
  }

  private List<Result> readRecords(int timestamp) {
    List<Result> results = new ArrayList<>();

    DistributedTransaction transaction = null;
    try {
      transaction = manager.start();
      Scan scan = SensorCommon.prepareScan(timestamp);
      results = transaction.scan(scan);
    } catch (TransactionException e) {
      // for Retry
      if (transaction != null) {
        try {
          transaction.abort();
        } catch (AbortException ex) {
          logWarn("abort failed.", ex);
        }
      }
      throw new RuntimeException("at least 1 record couldn't be read");
    }

    return results;
  }

  private int getIntFromPreviousState(String name) {
    int value = 0;
    if (getPreviousState().isNull(name)) {
      logWarn("There is no " + name + " since you use `--only-post`");
    } else {
      value = getPreviousState().getInt(name);
    }

    return value;
  }
}
