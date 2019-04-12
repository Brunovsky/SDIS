package dbs.transmitter;

import dbs.ChunkKey;
import dbs.Peer;
import dbs.files.FilesManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Handles one instance of the RESTORE peer protocol initiated by the TestApp. The
 * Restorer starts one Getchunker for each file chunk, and waits for their conclusion.
 * The Getchunkers signal their parent Restorer whenever they finish, successfully or not.
 */
public class Restorer implements Runnable {

  private final String pathname;
  private final String fileId;
  private final byte[][] chunks;
  private final int chunksNo;
  private final ConcurrentHashMap<ChunkKey,GetchunkTransmitter> instances;
  private final AtomicBoolean done = new AtomicBoolean(false);

  Restorer(String pathname, String fileId, int chunksNo) {
    this.pathname = pathname;
    this.fileId = fileId;
    this.chunks = new byte[chunksNo][];
    this.chunksNo = chunksNo;
    this.instances = new ConcurrentHashMap<>();

    RestoreHandler.getInstance().restorerPool.submit(this);
  }

  @Override
  public void run() {
    // Create getchunkers
    for (int no = 0; no < chunksNo; ++no) {
      ChunkKey key = new ChunkKey(fileId, no);
      GetchunkTransmitter getchunker = new GetchunkTransmitter(key, this);
      instances.put(key, getchunker);
      RestoreHandler.getInstance().getchunkers.put(key, getchunker);
    }

    // Submit getchunkers
    for (GetchunkTransmitter getchunker : instances.values()) {
      getchunker.submit();
    }
  }

  void failed(ChunkKey key) {
    if (done.getAndSet(true)) return;
    instances.remove(key);
    for (GetchunkTransmitter getchunker : instances.values()) {
      getchunker.cancel();
    }
    RestoreHandler.getInstance().restorers.remove(fileId);
    Peer.log("Failed to restore file " + pathname, Level.WARNING);
  }

  synchronized void assigned(ChunkKey key) {
    if (done.get()) return;
    GetchunkTransmitter getchunker = instances.get(key);
    chunks[key.getChunkNo()] = getchunker.getChunk();
    instances.remove(key);

    if (instances.isEmpty()) succeed();
  }

  private void succeed() {
    done.set(true);
    FilesManager.getInstance().putRestore(pathname, chunks);
    RestoreHandler.getInstance().restorers.remove(fileId);
    Peer.log("Successfully restored file " + pathname, Level.INFO);
  }
}