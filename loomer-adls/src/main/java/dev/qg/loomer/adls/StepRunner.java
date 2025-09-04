package dev.qg.loomer.adls;

import com.azure.core.util.BinaryData;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.specialized.DataLakeLeaseClient;
import com.azure.storage.file.datalake.specialized.DataLakeLeaseClientBuilder;
import com.azure.core.util.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.qg.loomer.core.RetryPolicy;
import dev.qg.loomer.core.StepDef;
import dev.qg.loomer.core.Task;
import dev.qg.loomer.core.TaskContext;
import dev.qg.loomer.core.TaskRegistry;
import dev.qg.loomer.core.TaskResult;
import dev.qg.loomer.core.TaskResult.Status;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Run a claimed step with heartbeat and finalization against ADLS. */
public class StepRunner {
  private final TaskRegistry registry;
  private final RetryPolicy retryPolicy;
  private final DataLakeFileSystemClient fs;
  private final ObjectMapper mapper = new ObjectMapper();

  public StepRunner(TaskRegistry registry, RetryPolicy retryPolicy, DataLakeFileSystemClient fs) {
    this.registry = registry;
    this.retryPolicy = retryPolicy;
    this.fs = fs;
  }

  public TaskResult execute(AdlsCoordinator.ClaimedStep claim) throws Exception {
    DataLakeFileClient running = claim.file();
    StepDef def;
    try (InputStream stream = running.openInputStream().getInputStream()) {
      def = mapper.readValue(new String(stream.readAllBytes(), StandardCharsets.UTF_8), StepDef.class);
    }

    String runId = def.runId();
    String stepId = def.stepId();
    Task task = registry.resolve(def.taskType());
    TaskContext ctx = new AdlsTaskContext(runId, stepId, fs, mapper);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    Future<TaskResult> f = exec.submit(() -> task.run(ctx));
    TaskResult res;
    try {
      res = f.get(def.timeoutMs(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw e.getCause();
    } catch (Exception e) {
      f.cancel(true);
      res = TaskResult.retry("timeout");
    } finally {
      exec.shutdownNow();
    }

    switch (res.status()) {
      case SUCCESS -> finalizeDone(running, runId, stepId, Status.SUCCESS, claim.leaseId());
      case SKIPPED -> finalizeDone(running, runId, stepId, Status.SKIPPED, claim.leaseId());
      case FAIL -> finalizeDone(running, runId, stepId, Status.FAIL, claim.leaseId());
      case RETRY -> handleRetry(running, def, claim.leaseId());
    }
    return res;
  }

  private void finalizeDone(
      DataLakeFileClient runningFile, String runId, String stepId, Status status, String leaseId)
      throws IOException {
    String dest =
        "runs/" + runId + "/done/" + status.name().toLowerCase() + "/" + runningFile.getFileName();
    DataLakeRequestConditions conds = new DataLakeRequestConditions().setLeaseId(leaseId);
    runningFile.renameWithResponse(dest, leaseId, conds, null, null, Context.NONE);
    DataLakeLeaseClient leaseClient =
        new DataLakeLeaseClientBuilder().fileClient(runningFile).leaseId(leaseId).buildClient();
    leaseClient.releaseLease();
    new EventsAppender(fs, runId, mapper).append(stepId, status.name().toLowerCase());
  }

  private void handleRetry(DataLakeFileClient runningFile, StepDef def, String leaseId)
      throws IOException {
    int nextAttempt = def.attempts() + 1;
    if (!retryPolicy.shouldRetry(nextAttempt)) {
      finalizeDone(runningFile, def.runId(), def.stepId(), Status.FAIL, leaseId);
      return;
    }
    Instant due = retryPolicy.nextRetryAt(nextAttempt);
    StepDef updated =
        new StepDef(
            def.runId(),
            def.stepId(),
            def.taskType(),
            def.dependsOn(),
            def.remainingDeps(),
            due,
            nextAttempt,
            def.timeoutMs(),
            def.maxAttempts(),
            def.inputRef(),
            def.outputRef(),
            def.idempotencyKey());
    String bucket = Buckets.minuteShard(due, def.stepId());
    String readyPath =
        "runs/" + def.runId() + "/ready/" + bucket + "/" + runningFile.getFileName();
    fs.getDirectoryClient("runs/" + def.runId() + "/ready/" + bucket).createIfNotExists();
    fs.getFileClient(readyPath)
        .upload(BinaryData.fromString(mapper.writeValueAsString(updated)), true);
    DataLakeRequestConditions conds = new DataLakeRequestConditions().setLeaseId(leaseId);
    runningFile.deleteWithResponse(conds, null, Context.NONE);
    new DataLakeLeaseClientBuilder().fileClient(runningFile).leaseId(leaseId).buildClient().releaseLease();
  }
}
