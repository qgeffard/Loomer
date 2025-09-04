package dev.qg.loomer.adls;

import com.azure.core.util.BinaryData;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.qg.loomer.core.CancelledException;
import dev.qg.loomer.core.TaskContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Task context backed by Azure Data Lake Storage. */
public class AdlsTaskContext implements TaskContext {
  private final String runId;
  private final String stepId;
  private final DataLakeFileSystemClient fs;
  private final ObjectMapper mapper;

  public AdlsTaskContext(
      String runId, String stepId, DataLakeFileSystemClient fs, ObjectMapper mapper) {
    this.runId = runId;
    this.stepId = stepId;
    this.fs = fs;
    this.mapper = mapper;
  }

  @Override
  public String runId() { return runId; }

  @Override
  public String stepId() { return stepId; }

  @Override
  public <T> T readInput(Class<T> type) throws IOException {
    String path = "runs/" + runId + "/io/" + stepId + "/in.json";
    DataLakeFileClient in = fs.getFileClient(path);
    if (!in.exists()) {
      return null;
    }
    return mapper.readValue(new String(in.readAllBytes(), StandardCharsets.UTF_8), type);
  }

  @Override
  public void writeOutput(Object value) throws IOException {
    String path = "runs/" + runId + "/io/" + stepId + "/out.json";
    DataLakeFileClient out = fs.getFileClient(path);
    fs.getDirectoryClient("runs/" + runId + "/io/" + stepId).createIfNotExists();
    out.upload(BinaryData.fromString(mapper.writeValueAsString(value)), true);
  }

  @Override
  public void checkCancelled() throws CancelledException {
    String path = "runs/" + runId + "/control/cancel.flag";
    if (fs.getFileClient(path).exists()) {
      throw new CancelledException("run cancelled");
    }
  }
}
