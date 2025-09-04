package dev.qg.loomer.adls;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;

/**
 * Scans running steps and returns them to ready if lease expired.
 *
 * <p>This is a simplified placeholder implementation; production code should
 * check lease expiry via {@code PathProperties.getLeaseDuration()} and rename
 * back to a ready bucket.</p>
 */
public class Scavenger {
  private final DataLakeFileSystemClient fs;

  public Scavenger(DataLakeFileSystemClient fs) {
    this.fs = fs;
  }

  public void sweep() {
    // TODO: implement lease check and reset logic
  }
}
