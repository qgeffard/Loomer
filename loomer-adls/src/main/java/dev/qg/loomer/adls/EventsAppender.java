package dev.qg.loomer.adls;

import com.azure.core.util.BinaryData;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.PathProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

/** Append events to an NDJSON file in ADLS. */
public class EventsAppender {
  private final DataLakeFileClient file;
  private final ObjectMapper mapper;

  public EventsAppender(DataLakeFileSystemClient fs, String runId, ObjectMapper mapper) {
    this.file = fs.getFileClient("runs/" + runId + "/events/events.ndjson");
    this.mapper = mapper;
    fs.getDirectoryClient("runs/" + runId + "/events").createIfNotExists();
    if (!file.exists()) {
      file.create();
    }
  }

  public synchronized long append(String stepId, String status) throws IOException {
    String json = mapper.writeValueAsString(
            Map.of("ts", Instant.now().toString(), "step", stepId, "status", status))
        + "\n";
    PathProperties props = file.getProperties();
    long pos = props.getFileSize();
    file.append(BinaryData.fromString(json), pos);
    file.flush(pos + json.getBytes(StandardCharsets.UTF_8).length);
    return pos;
  }
}
