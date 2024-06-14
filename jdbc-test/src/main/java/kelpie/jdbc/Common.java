package kelpie.jdbc;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.time.Duration;

public class Common {
  private static final int WAIT_MILLS = 1000;
  private static final long SLEEP_BASE_MILLIS = 100L;
  private static final int MAX_RETRIES = 10;

  public static Retry getRetryWithFixedWaitDuration(String name) {
    return getRetryWithFixedWaitDuration(name, MAX_RETRIES, WAIT_MILLS);
  }

  public static Retry getRetryWithFixedWaitDuration(String name, int maxRetries, int waitMillis) {
    RetryConfig retryConfig =
        RetryConfig.custom()
            .maxAttempts(maxRetries)
            .waitDuration(Duration.ofMillis(waitMillis))
            .build();

    return Retry.of(name, retryConfig);
  }

  public static Retry getRetryWithExponentialBackoff(String name) {
    IntervalFunction intervalFunc = IntervalFunction.ofExponentialBackoff(SLEEP_BASE_MILLIS, 2.0);

    RetryConfig retryConfig =
        RetryConfig.custom().maxAttempts(MAX_RETRIES).intervalFunction(intervalFunc).build();

    return Retry.of(name, retryConfig);
  }
}
