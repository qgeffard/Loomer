package dev.qg.loomer.api;

import com.azure.core.util.BinaryData;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WorkflowController {
  private final ObjectMapper mapper = new ObjectMapper();
  private final DataLakeFileSystemClient fs;

  public WorkflowController() {
    String endpoint = System.getenv("ADLS_ENDPOINT");
    String fsName = System.getenv("ADLS_FS");
    ClientSecretCredential cred =
        new ClientSecretCredentialBuilder()
            .tenantId(System.getenv("AZURE_TENANT_ID"))
            .clientId(System.getenv("AZURE_CLIENT_ID"))
            .clientSecret(System.getenv("AZURE_CLIENT_SECRET"))
            .build();
    this.fs =
        new DataLakeServiceClientBuilder().endpoint(endpoint).credential(cred).buildClient()
            .getFileSystemClient(fsName);
  }

  @PostMapping(value = "/workflows", produces = MediaType.APPLICATION_JSON_VALUE)
  public Map<String, String> create(@RequestBody(required = false) Map<String, Object> body)
      throws IOException {
    String runId = UUID.randomUUID().toString();
    fs.getDirectoryClient("runs/" + runId + "/ready").create();
    return Map.of("runId", runId);
  }

  @GetMapping("/runs/{runId}")
  public Map<String, Object> status(@PathVariable String runId) {
    Map<String, Object> result = new HashMap<>();
    result.put("runId", runId);
    result.put("ready", count("runs/" + runId + "/ready"));
    result.put("running", count("runs/" + runId + "/running"));
    result.put("done", count("runs/" + runId + "/done"));
    return result;
  }

  private long count(String path) {
    long total = 0;
    for (PathItem item : fs.listPaths(new ListPathsOptions().setPath(path).setRecursive(true), null)) {
      if (Boolean.FALSE.equals(item.isDirectory())) {
        total++;
      }
    }
    return total;
  }

  @PostMapping("/runs/{runId}/cancel")
  public void cancel(@PathVariable String runId) {
    fs.getDirectoryClient("runs/" + runId + "/control").createIfNotExists();
    fs.getFileClient("runs/" + runId + "/control/cancel.flag")
        .upload(BinaryData.fromString(""), true);
  }

  @PostMapping(value = "/runs/{runId}/signal", consumes = MediaType.APPLICATION_JSON_VALUE)
  public void signal(@PathVariable String runId, @RequestBody Map<String, Object> payload)
      throws IOException {
    String dir = "runs/" + runId + "/control/signals";
    fs.getDirectoryClient(dir).createIfNotExists();
    String id = UUID.randomUUID().toString();
    fs.getFileClient(dir + "/" + id + ".json")
        .upload(BinaryData.fromString(mapper.writeValueAsString(payload)), true);
  }
}
