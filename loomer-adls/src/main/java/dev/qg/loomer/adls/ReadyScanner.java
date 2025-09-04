package dev.qg.loomer.adls;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Lists ready step files in ADLS. */
public class ReadyScanner {
  private final DataLakeFileSystemClient fs;

  public ReadyScanner(DataLakeFileSystemClient fs) {
    this.fs = fs;
  }

  public List<String> scan(Instant now) {
    List<String> result = new ArrayList<>();
    for (PathItem item : fs.listPaths(new ListPathsOptions().setRecursive(true), null)) {
      if (Boolean.FALSE.equals(item.isDirectory())
          && item.getName().contains("/ready/")
          && item.getName().endsWith(".json")) {
        result.add(item.getName());
      }
    }
    return result;
  }
}
