package dbs.transmitter;

import dbs.ChunkKey;
import dbs.Configuration;
import dbs.Peer;
import dbs.files.FileInfoManager;
import dbs.message.Message;

import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Level;

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
  final ConcurrentHashMap<ChunkKey,RemovedWaiter> waiters;

  final ConcurrentHashMap<ChunkKey,RemovedTransmitter> removers;

  final ScheduledThreadPoolExecutor waiterPool;

  final ScheduledThreadPoolExecutor removerPool;

  private ReclaimHandler() {
    this.waiters = new ConcurrentHashMap<>();
    this.waiterPool = new ScheduledThreadPoolExecutor(Configuration.waiterPoolSize);
    this.removers = new ConcurrentHashMap<>();
    this.removerPool = new ScheduledThreadPoolExecutor(Configuration.removerPoolSize);
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
  public RemovedWaiter receiveREMOVED(Message message) {
    String fileId = message.getFileId();
    int chunkNo = message.getChunkNo();
    Long senderId = Long.parseLong(message.getSenderId());
    ChunkKey key = new ChunkKey(fileId, chunkNo);

    // Exit immediately if we don't have the chunk.
    if (!FileInfoManager.getInstance().hasChunk(fileId, chunkNo)) return null;

    FileInfoManager.getInstance().removeBackupPeer(fileId, chunkNo, senderId);
    int perce = FileInfoManager.getInstance().getChunkReplicationDegree(fileId, chunkNo);
    int expec = FileInfoManager.getInstance().getDesiredReplicationDegree(fileId);

    if (perce >= expec) return null;
    Peer.log("Creating a RemovedWaiter for " + key, Level.INFO);

    return waiters.computeIfAbsent(key, RemovedWaiter::new);
  }

  public synchronized void initReclaim(long maxDiskSpaceKB) {
    long currentMaxKB = Configuration.storageCapacityKB;
    Configuration.storageCapacityKB = maxDiskSpaceKB;

    // The total capacity increased!
    if (currentMaxKB <= maxDiskSpaceKB) {
      if (currentMaxKB < maxDiskSpaceKB)
        Peer.log("Peer's maximum storage capacity increased from " + currentMaxKB + " to "
            + maxDiskSpaceKB, Level.INFO);
      else
        Peer.log("Peer's maximum storage capacity matches requested " + maxDiskSpaceKB,
            Level.INFO);
      return;
    }

    // Need to trim the backup system.
    TreeSet<ChunkKey> removeTree = FileInfoManager.getInstance().trimBackup();

    for (ChunkKey key : removeTree) {
      FileInfoManager.getInstance().deleteChunk(key.getFileId(), key.getChunkNo());
      removers.computeIfAbsent(key, RemovedTransmitter::new);
    }
  }

  void alertPUTCHUNK(Message message) {
    ChunkKey key = new ChunkKey(message.getFileId(), message.getChunkNo());
    RemovedWaiter waiter = waiters.get(key);
    if (waiter != null) waiter.detect();
  }
}
