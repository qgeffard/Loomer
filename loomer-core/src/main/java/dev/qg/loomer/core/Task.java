package dev.qg.loomer.core;

/** SPI for workflow tasks. */
public interface Task {
  TaskResult run(TaskContext ctx) throws Exception;
}
