package dbs;

import java.util.Objects;

public class ChunkKey implements Comparable<ChunkKey> {

  private final String fileId;
  private final int chunkNo;

  public ChunkKey(String fileId, int chunkNo) {
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
    return "chunk(" + fileId.substring(0, 10) + "," + chunkNo + ')';
  }

  @Override
  public int compareTo(ChunkKey other) {
    assert other != null;
    if (this == other) return 0;
    if (!fileId.equals(other.fileId)) {
      return fileId.compareTo(other.fileId);
    }
    return chunkNo - other.chunkNo;
  }
}
