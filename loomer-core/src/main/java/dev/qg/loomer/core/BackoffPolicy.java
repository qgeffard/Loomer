package dev.qg.loomer.core;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/** Exponential backoff with full jitter. */
public class BackoffPolicy {
  private final Duration base;
  private final Duration max;

  public BackoffPolicy(Duration base, Duration max) {
    this.base = base;
    this.max = max;
  }

  public Duration calculateDelay(int attempt) {
    long exp = 1L << (attempt - 1);
    long baseMs = base.toMillis();
    long delay = Math.min(max.toMillis(), exp * baseMs + ThreadLocalRandom.current().nextLong(baseMs));
    return Duration.ofMillis(delay);
  }
}
