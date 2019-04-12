package dbs.transmitter;

import dbs.ChunkKey;
import dbs.Configuration;
import dbs.Peer;
import dbs.Utils;
import dbs.fileInfoManager.FileInfoManager;
import dbs.message.Message;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class RestoreHandler {

  private static RestoreHandler handler;

  public static RestoreHandler getInstance() {
    assert handler != null;
    return handler;
  }

  public static RestoreHandler createInstance() {
    return handler == null ? (handler = new RestoreHandler()) : handler;
  }

  /**
   * A registry of active Chunkers. Whenever a Chunker is launched for a given CHUNK,
   * it registers itself here. Then the chunk receive handler can alert it that its
   * CHUNK was already multicasted on the network, and is no longer expected. The
   * Chunker has the responsibility of inserting and removing itself from this map.
   * This map is used by the Chunker and the chunk receive handler.
   * Entries in this map are never null.
   */
  final ConcurrentHashMap<ChunkKey,ChunkTransmitter> chunkers;

  /**
   * A registry of active Getchunkers. Whenever a Getchunker is launched for a given
   * CHUNK, it registers itself here. Then the chunk receive handler can assign it the
   * desired CHUNK when it is received. The Getchunker has the responsibility of
   * inserting and removing itself from this map.
   * This map is used by the Getchunker and the chunk receive handler.
   * Entries in this map are never null.
   */
  final ConcurrentHashMap<ChunkKey,GetchunkTransmitter> getchunkers;

  /**
   * A registry of active Restorers. Whenever a Restorer is launched for a given FILEID,
   * it registers itself here.
   * Entries in this map are never null.
   */
  final ConcurrentHashMap<String,Restorer> restorers;

  final ScheduledThreadPoolExecutor chunkPool;

  final ScheduledThreadPoolExecutor getchunkPool;

  final ScheduledThreadPoolExecutor restorerPool;

  private RestoreHandler() {
    this.chunkers = new ConcurrentHashMap<>();
    this.getchunkers = new ConcurrentHashMap<>();
    this.restorers = new ConcurrentHashMap<>();
    this.chunkPool = new ScheduledThreadPoolExecutor(Configuration.chunkPoolSize);
    this.getchunkPool = new ScheduledThreadPoolExecutor(Configuration.getChunkPoolSize);
    this.restorerPool = new ScheduledThreadPoolExecutor(Configuration.restorerPoolSize);
  }

  /**
   * Called whenever a GETCHUNK message proper is received.
   * Here we create a new Chunker for this message or return the currently running one
   * for the same chunk. If we don't have the chunk, we return null and do nothing.
   *
   * @param message The GETCHUNK message received. Presumed valid GETCHUNK message.
   * @return The Chunker responsible for managing this GETCHUNK, or null if we don't
   * have this chunk.
   */
  public ChunkTransmitter receiveGETCHUNK(Message message) {
    String fileId = message.getFileId();
    int chunkNo = message.getChunkNo();
    ChunkKey key = new ChunkKey(fileId, chunkNo);

    // Exit immediately if we don't have the chunk.
    if (!FileInfoManager.getInstance().hasChunk(fileId, chunkNo)) return null;

    return chunkers.computeIfAbsent(key, ChunkTransmitter::new);
  }

  /**
   * Called whenever a CHUNK message proper is received.
   * If we have a running Getchunker for this chunk we assign it the received chunk; if
   * we have a running Chunker we alert it that the chunk has been detect.
   *
   * @param message The CHUNK message received. Presumed valid CHUNK message.
   */
  public void receiveCHUNK(Message message) {
    String fileId = message.getFileId();
    int chunkNo = message.getChunkNo();
    byte[] bytes = message.getBody();
    ChunkKey key = new ChunkKey(fileId, chunkNo);

    // Update Getchunker
    GetchunkTransmitter getchunker = getchunkers.get(key);
    if (getchunker != null) getchunker.assign(bytes);

    // Update Chunker
    ChunkTransmitter chunker = chunkers.get(key);
    if (chunker != null) chunker.detect();
  }

  /**
   * Called to create a RESTORE initiator that will retrieve all chunks for a given
   * file id. It will create all necessary Getchunkers and wait for all of them.
   */
  public Restorer initRestore(String pathname) {
    try {
      File file = new File(pathname);
      String fileId = Utils.hash(file, Peer.getInstance().getId());
      int chunksNo = Utils.numberOfChunks(file.length());
      return restorers.computeIfAbsent(fileId, id -> new Restorer(pathname, id,
          chunksNo));
    } catch (Exception e) {
      return null;
    }
  }
}
