package dev.qg.loomer.core;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.RepeatedTest;

class BackoffPolicyTest {

  @RepeatedTest(5)
  void exponentialWithJitter() {
    BackoffPolicy bp = new BackoffPolicy(Duration.ofMillis(100), Duration.ofMillis(1000));
    long d1 = bp.calculateDelay(1).toMillis();
    assertTrue(d1 >= 100 && d1 <= 200);
    long d3 = bp.calculateDelay(3).toMillis();
    assertTrue(d3 <= 1000);
  }
}
