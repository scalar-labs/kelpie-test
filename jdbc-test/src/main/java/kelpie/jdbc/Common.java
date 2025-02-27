package kelpie.jdbc;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.time.Duration;

public class Common {

  public static Retry getRetryWithFixedWaitDuration(String name, int maxRetries, int waitMillis) {
    RetryConfig retryConfig =
        RetryConfig.custom()
            .maxAttempts(maxRetries)
            .waitDuration(Duration.ofMillis(waitMillis))
            .build();

    return Retry.of(name, retryConfig);
  }
}
