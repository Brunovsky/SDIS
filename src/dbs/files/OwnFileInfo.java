package dbs.files;

import java.io.File;
import java.io.Serializable;

public class OwnFileInfo extends FileInfo implements Serializable {

  private final String pathname;
  private final Integer numberOfChunks;

  /**
   * Constructs a new object of the OwnFileInfo class.
   */
  OwnFileInfo(String pathname, String fileId, int numberOfChunks) {
    super(fileId);
    this.pathname = pathname;
    this.numberOfChunks = numberOfChunks;
  }

  /**
   * Constructs a new object of the OwnFileInfo class, given the desired replication
   * degree for that file.
   *
   * @param desiredReplicationDegree The desired replication degree for that file.
   */
  OwnFileInfo(String pathname, String fileId, int numberOfChunks,
              Integer desiredReplicationDegree) {
    super(fileId, desiredReplicationDegree);
    this.pathname = pathname;
    this.numberOfChunks = numberOfChunks;
  }

  /**
   * @return This file's original pathname.
   */
  public String getPathname() {
    return this.pathname;
  }

  /**
   * @return This file's original chunk count.
   */
  public int getNumberOfChunks() {
    return this.numberOfChunks;
  }

  /**
   * @return The pathname as a file.
   */
  public File getFile() {
    return new File(pathname);
  }

  @Override
  public String toString() {
    return ' ' + pathname + "\n  Number of chunks: " + numberOfChunks
        + '\n' + super.toString();
  }
}
