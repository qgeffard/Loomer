package dev.qg.loomer.core;

import java.time.Instant;
import java.util.List;

/** Definition of a workflow step. */
public record StepDef(
    String runId,
    String stepId,
    String taskType,
    List<String> dependsOn,
    int remainingDeps,
    Instant dueAt,
    int attempts,
    long timeoutMs,
    int maxAttempts,
    String inputRef,
    String outputRef,
    String idempotencyKey) {}
