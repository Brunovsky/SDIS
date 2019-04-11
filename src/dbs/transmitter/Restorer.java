package dbs.transmitter;


import dbs.ChunkKey;
import dbs.fileInfoManager.FileInfoManager;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles one instance of the RESTORE peer protocol initiated by the TestApp. The
 * Restorer starts one GetchunkTransmitter for each file chunk, and waiting for their
 * conclusion. The Getchunkers signal their parent Restorer whenever they finish,
 * successfully or not.
 */
public class Restorer {

  private final String pathname;
  private final String fileId;
  private final byte[][] chunks;
  private final ConcurrentHashMap<ChunkKey,GetchunkTransmitter> instances;

  Restorer(String pathname, String fileId, int chunksNo) {
    this.pathname = pathname;
    this.fileId = fileId;
    this.chunks = new byte[chunksNo][];
    this.instances = new ConcurrentHashMap<>();

    for (int no = 0; no < chunksNo; ++no) {
      ChunkKey key = new ChunkKey(fileId, no);
      GetchunkTransmitter getchunker = new GetchunkTransmitter(key, this);
      instances.put(key, getchunker);
      RestoreHandler.getInstance().getchunkers.put(key, getchunker);
    }

    for (GetchunkTransmitter getchunker : instances.values()) {
      getchunker.submit();
    }
  }

  void failed(ChunkKey key) {
    for (GetchunkTransmitter getchunker : instances.values()) {
      getchunker.cancel();
    }
    RestoreHandler.getInstance().restorers.remove(fileId);
  }

  void assigned(ChunkKey key) {
    GetchunkTransmitter getchunker = instances.get(key);
    chunks[key.getChunkNo()] = getchunker.getChunk();
    instances.remove(key);

    if (instances.isEmpty()) {
      FileInfoManager.getInstance().putRestore(pathname, chunks);
      RestoreHandler.getInstance().restorers.remove(fileId);
    }
  }
}