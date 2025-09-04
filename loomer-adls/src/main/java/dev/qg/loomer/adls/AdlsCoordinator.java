package dev.qg.loomer.adls;

import com.azure.core.util.BinaryData;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.DataLakeLeaseClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.qg.loomer.core.StepDef;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Coordinate step claims and dependency handling using Azure Data Lake Storage.
 */
public class AdlsCoordinator {
  private final DataLakeFileSystemClient fs;
  private final ObjectMapper mapper = new ObjectMapper();

  public AdlsCoordinator(DataLakeFileSystemClient fs) {
    this.fs = fs;
  }

  /** Atomically claim a ready step by renaming and leasing the file. */
  public ClaimedStep tryClaim(String readyPath, String workerId) {
    DataLakeFileClient ready = fs.getFileClient(readyPath);
    String runningPath = readyPath.replaceFirst("ready/", "running/" + workerId + "/");
    DataLakeFileClient running = fs.getFileClient(runningPath);
    try {
      ready.rename(runningPath);
      DataLakeLeaseClient leaseClient = running.getLeaseClient();
      String leaseId = leaseClient.acquireLease(30).getValue().getLeaseId();
      return new ClaimedStep(running, leaseId);
    } catch (DataLakeStorageException e) {
      if (e.getStatusCode() == 409 || e.getStatusCode() == 412) {
        return null;
      }
      throw e;
    }
  }

  /**
   * Decrement dependency counter and enqueue child step when reaching zero.
   */
  public String decrementDependency(String depPath, StepDef child) throws IOException {
    DataLakeFileClient dep = fs.getFileClient(depPath);
    int val = Integer.parseInt(new String(dep.readAllBytes(), StandardCharsets.UTF_8).trim());
    val--;
    if (val <= 0) {
      dep.delete();
      String bucket = Buckets.minuteShard(Instant.now(), child.stepId());
      String readyPath =
          "runs/" + child.runId() + "/ready/" + bucket + "/step-" + child.stepId() + ".json";
      fs.getDirectoryClient("runs/" + child.runId() + "/ready/" + bucket).createIfNotExists();
      fs.getFileClient(readyPath)
          .upload(BinaryData.fromString(mapper.writeValueAsString(child)), true);
      return readyPath;
    } else {
      dep.upload(BinaryData.fromString(Integer.toString(val)), true);
      return null;
    }
  }

  public record ClaimedStep(DataLakeFileClient file, String leaseId) {}
}
