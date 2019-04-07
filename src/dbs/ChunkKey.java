package dbs;

import java.security.MessageDigest;

public class ChunkKey {
  private String fileId;
  private int chunkNumber;

  public ChunkKey(String fileId, int chunkNumber) {
    this.fileId = fileId;
    this.chunkNumber = chunkNumber;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ChunkKey))
      return false;

    ChunkKey objChunckKey = (ChunkKey) obj;
    return objChunckKey.chunkNumber == this.chunkNumber &&
        MessageDigest.isEqual(objChunckKey.fileId.getBytes(), this.fileId.getBytes());
  }
}
