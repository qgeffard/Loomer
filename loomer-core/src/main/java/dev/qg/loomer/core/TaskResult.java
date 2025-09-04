package dev.qg.loomer.core;

/** Result of a task execution. */
public record TaskResult(Status status, String message) {
  public enum Status { SUCCESS, RETRY, FAIL, SKIPPED }

  public static TaskResult success() {
    return new TaskResult(Status.SUCCESS, null);
  }

  public static TaskResult retry(String msg) {
    return new TaskResult(Status.RETRY, msg);
  }

  public static TaskResult fail(String msg) {
    return new TaskResult(Status.FAIL, msg);
  }

  public static TaskResult skipped(String msg) {
    return new TaskResult(Status.SKIPPED, msg);
  }
}
