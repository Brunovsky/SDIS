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
        if(obj == null || !(obj instanceof ChunkKey))
            return false;

        ChunkKey objChunckKey = (ChunkKey) obj;
        return objChunckKey.chunkNumber == this.chunkNumber &&
                MessageDigest.isEqual(objChunckKey.fileId, this.fileId);
    }
}
