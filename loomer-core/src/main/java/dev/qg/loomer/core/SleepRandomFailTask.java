package dev.qg.loomer.core;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/** Demo task that randomly fails requiring retry. */
public class SleepRandomFailTask implements Task {
  @Override
  public TaskResult run(TaskContext ctx) throws Exception {
    ctx.checkCancelled();
    Thread.sleep(20);
    if (ThreadLocalRandom.current().nextBoolean()) {
      return TaskResult.retry("random");
    }
    ctx.writeOutput(Map.of("ok", true));
    return TaskResult.success();
  }
}
