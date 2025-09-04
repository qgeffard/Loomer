package dev.qg.loomer.worker;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import dev.qg.loomer.adls.AdlsCoordinator;
import dev.qg.loomer.adls.ReadyScanner;
import dev.qg.loomer.adls.StepRunner;
import dev.qg.loomer.core.BackoffPolicy;
import dev.qg.loomer.core.RetryPolicy;
import dev.qg.loomer.core.SleepRandomFailTask;
import dev.qg.loomer.core.TaskRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** Minimal worker loop using Azure Data Lake Storage. */
public class WorkerMain {
  public static void main(String[] args) throws Exception {
    String endpoint = System.getenv("ADLS_ENDPOINT");
    String fsName = System.getenv("ADLS_FS");
    ClientSecretCredential cred =
        new ClientSecretCredentialBuilder()
            .tenantId(System.getenv("AZURE_TENANT_ID"))
            .clientId(System.getenv("AZURE_CLIENT_ID"))
            .clientSecret(System.getenv("AZURE_CLIENT_SECRET"))
            .build();
    DataLakeFileSystemClient fs =
        new DataLakeServiceClientBuilder().endpoint(endpoint).credential(cred).buildClient()
            .getFileSystemClient(fsName);

    TaskRegistry reg = new TaskRegistry();
    reg.register("sleep.randomFail", new SleepRandomFailTask());
    RetryPolicy retry =
        new RetryPolicy(3, new BackoffPolicy(Duration.ofMillis(100), Duration.ofSeconds(1)));
    ReadyScanner scanner = new ReadyScanner(fs);
    AdlsCoordinator coord = new AdlsCoordinator(fs);
    StepRunner runner = new StepRunner(reg, retry, fs);
    List<String> ready = scanner.scan(Instant.now());
    for (String path : ready) {
      var cs = coord.tryClaim(path, "worker");
      if (cs != null) {
        runner.execute(cs);
      }
    }
  }
}
