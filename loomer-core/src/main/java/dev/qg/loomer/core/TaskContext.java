package dev.qg.loomer.core;

public interface TaskContext {
  String runId();
  String stepId();
  <T> T readInput(Class<T> type) throws Exception;
  void writeOutput(Object value) throws Exception;
  void checkCancelled() throws CancelledException;
}
