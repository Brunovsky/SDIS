package dbs;

import java.security.MessageDigest;

public class ChunkKey {
  private byte[] fileId;
  private int chunkNumber;

  public ChunkKey(byte[] fileId, int chunkNumber) {
    this.fileId = fileId;
    this.chunkNumber = chunkNumber;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ChunkKey))
      return false;

    ChunkKey objChunkKey = (ChunkKey) obj;
    return objChunkKey.chunkNumber == this.chunkNumber &&
        MessageDigest.isEqual(objChunkKey.fileId, this.fileId);
  }
}
