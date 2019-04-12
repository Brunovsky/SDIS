package dbs.transmitter;

import dbs.ChunkKey;
import dbs.Configuration;
import dbs.fileInfoManager.FileInfoManager;
import dbs.message.Message;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

// TODO: THIS IS NOT FINISHED. lul
public class ReclaimHandler {

  private static ReclaimHandler handler;

  public static ReclaimHandler getInstance() {
    assert handler != null;
    return handler;
  }

  public static ReclaimHandler createInstance() {
    return handler == null ? (handler = new ReclaimHandler()) : handler;
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
  final ConcurrentHashMap<ChunkKey,@NotNull RemovedWaiter> waiters;

  final ScheduledThreadPoolExecutor waiterPool;

  private ReclaimHandler() {
    this.waiters = new ConcurrentHashMap<>();
    this.waiterPool = new ScheduledThreadPoolExecutor(Configuration.removedPoolSize);
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
    ChunkKey key = new ChunkKey(fileId, chunkNo);

    // Exit immediately if we don't have the chunk.
    if (!FileInfoManager.getInstance().hasChunk(fileId, chunkNo)) return null;

    int perce = FileInfoManager.getInstance().getChunkReplicationDegree(fileId, chunkNo);
    int expec = FileInfoManager.getInstance().getDesiredReplicationDegree(fileId);

    if (perce >= expec) return null;

    return waiters.computeIfAbsent(key, RemovedWaiter::new);
  }

  public void initReclaim(long maxDiskSpace) {
    // TODO...
  }
}
