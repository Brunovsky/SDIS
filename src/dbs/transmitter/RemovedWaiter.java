package dbs.transmitter;


import dbs.ChunkKey;
import dbs.Protocol;
import dbs.Utils;
import dbs.fileInfoManager.FileInfoManager;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RemovedWaiter implements Runnable {

  private final ChunkKey key;
  private Future scheduled;
  private AtomicBoolean done = new AtomicBoolean(false);

  /**
   * Construct a RemovedWaiter for a REMOVED message with this key.
   *
   * @param key The requested chunk identifier (and also the key in the waiters map)
   */
  RemovedWaiter(ChunkKey key) {
    this.key = key;
    int delay = Utils.getRandom(Protocol.minDelay, Protocol.maxDelay);
    this.scheduled = ReclaimHandler.getInstance().waiterPool.schedule(this, delay,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Finish the Waiter, removing it from the map to release memory.
   * Called if the Waiter decided to start a backup subprotocol instance for the
   * desired chunk.
   * Called by the owner thread.
   */
  private void end() {
    if (done.getAndSet(true)) return;
    ReclaimHandler.getInstance().waiters.remove(key);
  }

  /**
   * Finish the Waiter, removing it from the map to release memory.
   * Called by the putchunk receive handler if it receives a chunk with this Waiter's
   * key.
   * Called by the putchunk receiver.
   */
  void detect() {
    if (done.getAndSet(true)) return;
    ReclaimHandler.getInstance().waiters.remove(key);
    scheduled.cancel(true);
  }

  /**
   * @return This Waiter's key
   */
  public ChunkKey getKey() {
    return key;
  }

  /**
   * @return true if this Waiter has ended, and false otherwise.
   */
  public boolean isDone() {
    return done.get();
  }

  /**
   * Scheduled function, run when the Waiter is not aborted by the chunk receiver and it
   * decides to start the backup subprotocol.
   */
  @Override
  public void run() {
    if (done.get()) return;
    end();

    String fileId = key.getFileId();
    int chunkNo = key.getChunkNo();

    // Get the chunk. Ensure we still have it and no unexpected IO error occurred.
    byte[] chunk = FileInfoManager.getInstance().getChunk(fileId, chunkNo);
    if (chunk == null) return;

    // Get the replication degree. Ensure we still have it
    int perce = FileInfoManager.getInstance().getChunkReplicationDegree(fileId, chunkNo);
    int expec = FileInfoManager.getInstance().getDesiredReplicationDegree(fileId);

    if (perce < expec) {
      // TODO: LAUNCH PUTCHUNKER
    }
  }
}