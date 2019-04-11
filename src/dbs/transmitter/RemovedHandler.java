package dbs.transmitter;

import dbs.ChunkKey;
import dbs.Peer;
import dbs.Protocol;
import dbs.Utils;
import dbs.fileInfoManager.FileInfoManager;
import dbs.message.Message;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

// TODO: THIS IS NOT FINISHED. lul
public class RemovedHandler {

  private static RemovedHandler removedHandler;

  public static RemovedHandler getInstance() {
    assert removedHandler != null;
    return removedHandler;
  }

  public static RemovedHandler createInstance() {
    assert removedHandler == null;
    return removedHandler = new RemovedHandler();
  }

  /**
   * A registry of active RemovedWaiters. Whenever a RemovedWaiter is launched for a
   * given CHUNK, it registers itself here. Then the putchunk receive handler can alert
   * it that its PUTCHUNK message was already multicasted on the network, meaning the
   * backup subprotocol was already started by another peer, and so it is no longer
   * needed for this peer to initiate one himself.
   * This map is used by the RemovedWaiter and the putchunk receive handler.
   * Entries in this map are never null.
   */
  private final ConcurrentHashMap<ChunkKey,@NotNull RemovedWaiter> waiters;

  private RemovedHandler() {
    this.waiters = new ConcurrentHashMap<>();
  }

  public class RemovedWaiter implements Runnable {
    private final ChunkKey key;
    private Future scheduled;
    private boolean done;

    /**
     * Construct a RemovedWaiter for a REMOVED message with this key.
     *
     * @param key The requested chunk identifier (and also the key in the waiters map)
     */
    private RemovedWaiter(@NotNull ChunkKey key) {
      this.key = key;
      randomSchedule();
    }

    /**
     * Randomly schedule the PUTCHUNK message to be sent in the near future.
     */
    private void randomSchedule() {
      int delay = Utils.getRandom(Protocol.minDelay, Protocol.maxDelay);
      this.scheduled = Peer.getInstance().getPool().schedule(this, delay,
          TimeUnit.MILLISECONDS);
    }

    /**
     * Finish the Waiter, removing it from the map to release memory.
     * Called if the Waiter decided to start a backup subprotocol instance for the
     * desired chunk.
     */
    private synchronized void end() {
      if (done) return;
      done = true;
      waiters.remove(key); // TODO: KEEP IT TILL LATER ***
    }

    /**
     * Finish the Waiter, removing it from the map to release memory.
     * Called by the putchunk receive handler if it receives a chunk with this Waiter's
     * key.
     */
    synchronized void detect() {
      if (done) return;
      done = true;
      waiters.remove(key);
      scheduled.cancel(false);
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
    public synchronized boolean isDone() {
      return done;
    }

    /**
     * Scheduled function, run when the Waiter is not aborted by the chunk receiver and it
     * decides to start the backup subprotocol.
     */
    @Override
    public synchronized void run() {
      if (done) return;

      String fileId = key.getFileId();
      int chunkNo = key.getChunkNo();

      byte[] chunk = FileInfoManager.getInstance().getChunk(fileId, chunkNo);
      if (chunk == null) {
        end();
        return;
      }

      end();
      // TODO: Start BACKUP subprotocol sequence
    }
  }

  /**
   * Called whenever a REMOVED message proper is received.
   * Here we create a new or currently running RemovedWaiter for this message if necessary
   * -- which is when the actual replication degree of this chunk falls below the
   * required replication degree threshold. If a RemovedWaiter already exists a new
   * one will not be created.
   *
   * @param message The PUTCHUNK message received. Presumed valid PUTCHUNK message.
   * @return The RemovedWaiter responsible for managing the eventual PUTCHUNK
   * subprotocol instance, null in any other case.
   */
  public RemovedWaiter receiveREMOVED(@NotNull Message message) {
    String fileId = message.getFileId();
    int chunkNo = message.getChunkNo();

    // Exit immediately if we don't have the chunk.
    if (!FileInfoManager.getInstance().hasChunk(fileId, chunkNo)) return null;

    // Update the chunk count.
    // TODO: peer.fileInfoManager.decrementReplicationCount(fileId, chunkNo, senderId);
    // Get: current replication, expected replication
    // ...

    return null;
  }
}
