package dbs.files;

import java.io.File;

public class FileRequest {

  private File file;
  private String fileId;
  private Integer numberChunks;

  public FileRequest(File file, String fileId, Integer numberChunks) {
    this.file = file;
    this.fileId = fileId;
    this.numberChunks = numberChunks;
  }

  public File getFile() {
    return this.file;
  }

  public String getFileId() {
    return this.fileId;
  }

  public Integer getNumberChunks() {
    return this.numberChunks;
  }
}
