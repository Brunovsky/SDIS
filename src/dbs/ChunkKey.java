package dbs;

import java.security.MessageDigest;

public class ChunkKey {
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

    ChunkKey objChunckKey = (ChunkKey) obj;
    return objChunckKey.chunkNo == this.chunkNo &&
        MessageDigest.isEqual(objChunckKey.fileId.getBytes(), this.fileId.getBytes());
  }
}
