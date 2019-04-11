package dbs;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class ChunkKey {

  private final String fileId;
  private final int chunkNo;

  public ChunkKey(@NotNull String fileId, int chunkNo) {
    this.fileId = fileId;
    this.chunkNo = chunkNo;
  }

  public String getFileId() {
    return fileId;
  }

  public int getChunkNo() {
    return chunkNo;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ChunkKey))
      return false;

    ChunkKey other = (ChunkKey) obj;
    return this.chunkNo == other.chunkNo && fileId.equals(other.fileId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileId, chunkNo);
  }

  @Override
  public String toString() {
    return "ChunkKey{" + "fileId='" + fileId + '\'' + ", chunkNo=" + chunkNo + '}';
  }
}
