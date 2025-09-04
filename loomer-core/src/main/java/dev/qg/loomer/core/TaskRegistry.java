package dev.qg.loomer.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Simple registry for tasks. */
public class TaskRegistry {
  private final Map<String, Task> tasks = new ConcurrentHashMap<>();

  public void register(String type, Task task) {
    tasks.put(type, task);
  }

  public Task resolve(String type) {
    Task t = tasks.get(type);
    if (t == null) throw new IllegalArgumentException("Unknown task type " + type);
    return t;
  }
}
