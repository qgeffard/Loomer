package dev.qg.loomer.core;

import java.time.Instant;

/** Simple retry policy. */
public class RetryPolicy {
  private final int maxAttempts;
  private final BackoffPolicy backoff;

  public RetryPolicy(int maxAttempts, BackoffPolicy backoff) {
    this.maxAttempts = maxAttempts;
    this.backoff = backoff;
  }

  public boolean shouldRetry(int attempts) {
    return attempts < maxAttempts;
  }

  public Instant nextRetryAt(int attempt) {
    return Instant.now().plus(backoff.calculateDelay(attempt));
  }
}
