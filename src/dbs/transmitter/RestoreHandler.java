package dbs.transmitter;

import dbs.ChunkKey;
import dbs.Peer;
import dbs.Utils;
import dbs.message.Message;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class RestoreHandler {
  private static Logger LOGGER = Logger.getLogger(RestoreHandler.class.getName());
  private final Peer peer;

  /**
   * A registry of active Chunkers. Whenever a Chunker is launched for a given CHUNK,
   * it registers itself here. Then the chunk receive handler can alert it that its
   * CHUNK was already multicasted on the network, and is no longer expected. The
   * Chunker has the responsibility of inserting and removing itself from this map.
   * This map is used by the Chunker and the chunk receive handler.
   * Entries in this map are never null.
   */
  private final ConcurrentHashMap<ChunkKey,@NotNull Chunker> chunkers;

  /**
   * A registry of active Getchunkers. Whenever a Getchunker is launched for a given
   * CHUNK, it registers itself here. Then the chunk receive handler can assign it the
   * desired CHUNK when it is received. The Getchunker has the responsibility of
   * inserting and removing itself from this map.
   * This map is used by the Getchunker and the chunk receive handler.
   * Entries in this map are never null.
   */
  private final ConcurrentHashMap<ChunkKey,@NotNull Getchunker> getchunkers;

  /**
   * A registry of active Restorers. Whenever a Restorer is launched for a given FILEID,
   * it registers itself here.
   * Entries in this map are never null.
   */
  private final ConcurrentHashMap<String,@NotNull Restorer> restorers;

  public RestoreHandler(@NotNull Peer peer) {
    this.peer = peer;
    this.chunkers = new ConcurrentHashMap<>();
    this.getchunkers = new ConcurrentHashMap<>();
    this.restorers = new ConcurrentHashMap<>();
  }

  /**
   * Handles the response to a GETCHUNK message, as it must consider the possibility of
   * responding with a CHUNK message within a small window of time. The Chunker
   * registers himself in the chunkers map, to be alerted by the chunk receiver if
   * another CHUNK message of the same type is detected on the multicast network,
   * and promptly cancelled.
   * We provide Future-like functionality with isDone(), but this should not be used.
   * TODO: A Chunker instance is public only for testing purposes.
   */
  public class Chunker implements Runnable {
    private final ChunkKey key;
    private Future scheduled;
    private boolean done;

    /**
     * Construct a Chunker for a GETCHUNK message with this key.
     *
     * @param key The requested chunk identifier (and also the key in the chunkers map)
     */
    Chunker(@NotNull ChunkKey key) {
      this.key = key;
      randomSchedule();
    }

    /**
     * Randomly schedule the CHUNK message to be sent in the near future.
     */
    private void randomSchedule() {
      int time = peer.getConfig().waitChunk;
      this.scheduled = peer.getPool().schedule(this, time, TimeUnit.MILLISECONDS);
    }

    /**
     * Finish the Chunker, removing it from the map to release memory.
     * Called if the Chunker decided to respond with the requested chunk, regardless of
     * whether or not it succeeded.
     */
    synchronized void end() {
      if (done) return;
      done = true;
      chunkers.remove(key);
    }

    /**
     * Finish the Chunker, removing it from the map to release memory.
     * Called by the chunk receiver if it receives a chunk with this Chunker's key.
     */
    synchronized void detect() {
      if (done) return;
      done = true;
      chunkers.remove(key);
      scheduled.cancel(false);
    }

    /**
     * @return This Chunker's key
     */
    public ChunkKey getKey() {
      return key;
    }

    /**
     * @return true if this Chunker has ended (either way), and false otherwise
     */
    public synchronized boolean isDone() {
      return done;
    }

    /**
     * Scheduled function, run when the Chunker is not aborted by the chunk receiver
     * and it decides to send the message.
     */
    @Override
    public synchronized void run() {
      if (done) return;

      String fileId = key.getFileId();
      int chunkNo = key.getChunkNo();

      // Get the chunk. Ensure we still have it and no unexpected IO error occurred.
      byte[] chunk = peer.fileInfoManager.getChunk(fileId, chunkNo);
      if (chunk == null) {
        end();
        return;
      }

      end();
      Message message = Message.CHUNK(fileId, peer.getConfig().version, chunkNo, chunk);
      peer.send(message);
    }
  }

  /**
   * Handles one instance of the GETCHUNK protocol initiated by this peer. The
   * Getchunker starts by sending a GETCHUNK message to the multicast network, and
   * awaits its response in the form of an alert (assign) from the chunk receiver. If no
   * alert comes, it tries again, up to a maximum number of times before aborting.
   * TODO: A Getchunker instance is public only for testing purposes.
   */
  public class Getchunker implements Runnable {
    private final ChunkKey key;
    private final Restorer restorer;
    private final Message message;
    private volatile byte[] chunk;
    private int attempts = 0;
    private Future scheduled;
    private volatile boolean done;

    /**
     * Construct a Getchunker for this chunk key.
     *
     * @param key The request chunk's key (and also the key in the getchunkers map)
     */
    Getchunker(@NotNull ChunkKey key, @NotNull Restorer restorer) {
      String fileId = key.getFileId();
      int chunkNo = key.getChunkNo();

      this.key = key;
      this.restorer = restorer;
      this.message = Message.GETCHUNK(fileId, peer.getConfig().version, chunkNo);
    }

    /**
     * Schedule the GETCHUNK message to be resent in the near future.
     */
    private synchronized void schedule() {
      if (done) return;
      int time = peer.getConfig().waitGetchunk * (1 << attempts);
      scheduled = peer.getPool().schedule(this, time, TimeUnit.MILLISECONDS);
    }

    /**
     * Submit the Getchunker to the thread pool.
     */
    private synchronized void submit() {
      if (done) return;
      scheduled = peer.getPool().submit(this);
    }

    /**
     * Terminate this Getchunker unsuccessfully because the number of retries ran out.
     * Alert the parent Restorer to this fact, so it can cancel the other Getchunkers
     * of this file.
     */
    synchronized void end() {
      if (done) return;
      done = true;
      getchunkers.remove(key);
      restorer.failed(key);
    }

    /**
     * Terminate this Getchunker unsuccessfully because another Getchunker for the same
     * file ran out of retries. This is called by the parent Restorer.
     */
    synchronized void cancel() {
      if (done) return;
      done = true;
      getchunkers.remove(key);
      if (scheduled != null) scheduled.cancel(false);
    }

    /**
     * Terminate this Getchunker successfully, assigning the received chunk to the
     * promise, and alert the parent Restorer to the conclusion.
     *
     * @param received The chunk detected by the chunk receiver
     */
    synchronized void assign(byte @NotNull [] received) {
      if (done) return;
      done = true;
      getchunkers.remove(key);
      if (scheduled != null) scheduled.cancel(false);
      chunk = received;
      restorer.assigned(key);
    }

    /**
     * @return This Getchunker's key
     */
    public ChunkKey getKey() {
      return key;
    }

    /**
     * @return This Getchunker's retrieved chunk
     */
    public byte[] getChunk() {
      return chunk;
    }

    /**
     * @return true if this Chunker has ended (whichever way), and false otherwise
     */
    public boolean isDone() {
      return done;
    }

    /**
     * Scheduled function, run when the Getchunker is not aborted by the chunk receiver
     * and it decides to resend the GETCHUNK message. It may run out of retries.
     */
    @Override
    public synchronized void run() {
      if (done) return;

      // Ran out of attempts. Quit (done status) without having received the chunk.
      if (attempts++ > peer.getConfig().maxGetchunkAttempts) {
        end();
        return;
      }

      // Try again
      peer.send(message);
      schedule();
    }
  }

  /**
   * Handles one instance of the RESTORE peer protocol initiated by the TestApp. The
   * Restorer starts one Getchunker for each file chunk, and waiting for their conclusion.
   * The Getchunkers signal their parent Restorer whenever they finish, successfully or
   * not.
   */
  public class Restorer {
    private final String pathname;
    private final String fileId;
    private final byte[][] chunks;
    private final ConcurrentHashMap<ChunkKey,@NotNull Getchunker> instances;

    Restorer(@NotNull String pathname, @NotNull String fileId, int chunksNo) {
      this.pathname = pathname;
      this.fileId = fileId;
      this.chunks = new byte[chunksNo][];
      this.instances = new ConcurrentHashMap<>();

      for (int no = 0; no < chunksNo; ++no) {
        ChunkKey key = new ChunkKey(fileId, no);
        Getchunker getchunker = new Getchunker(key, this);
        instances.put(key, getchunker);
        getchunkers.put(key, getchunker);
      }

      for (Getchunker getchunker : instances.values()) {
        getchunker.submit();
      }
    }

    private void failed(@NotNull ChunkKey key) {
      for (Getchunker getchunker : instances.values()) {
        getchunker.cancel();
      }
      int chunkNo = key.getChunkNo();
      LOGGER.warning("Failed to restore chunk " + chunkNo + " for file " + pathname);
      restorers.remove(fileId);
    }

    private void assigned(@NotNull ChunkKey key) {
      Getchunker getchunker = instances.get(key);
      chunks[key.getChunkNo()] = getchunker.getChunk();
      instances.remove(key);

      if (instances.isEmpty()) {
        peer.fileInfoManager.putRestore(pathname, chunks);
        LOGGER.info("Restored file " + pathname);
        restorers.remove(fileId);
      }
    }
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
  public Chunker receiveGETCHUNK(@NotNull Message message) {
    String fileId = message.getFileId();
    int chunkNo = message.getChunkNo();
    ChunkKey key = new ChunkKey(fileId, chunkNo);

    LOGGER.info("Received GETCHUNK message for " + key);

    // Exit immediately if we don't have the chunk.
    if (!peer.fileInfoManager.hasChunk(fileId, chunkNo)) return null;

    return chunkers.computeIfAbsent(key, Chunker::new);
  }

  /**
   * Called whenever a CHUNK message proper is received.
   * If we have a running Getchunker for this chunk we assign it the received chunk; if
   * we have a running Chunker we alert it that the chunk has been detect.
   *
   * @param message The CHUNK message received. Presumed valid CHUNK message.
   */
  public void receiveCHUNK(@NotNull Message message) {
    String fileId = message.getFileId();
    int chunkNo = message.getChunkNo();
    byte[] bytes = message.getBody();
    ChunkKey key = new ChunkKey(fileId, chunkNo);

    LOGGER.info("Received CHUNK message for " + key);

    // Update Getchunker
    Getchunker getchunker = getchunkers.get(key);
    if (getchunker != null) getchunker.assign(bytes);

    // Update Chunker
    Chunker chunker = chunkers.get(key);
    if (chunker != null) chunker.detect();
  }

  /**
   * Called to create a RESTORE initiator that will retrieve all chunks for a given
   * file id. It will create all necessary Getchunkers and wait for all of them.
   */
  private Restorer initRestorer(@NotNull String pathname) throws Exception {
    File file = new File(pathname);
    String fileId = Utils.hash(file, peer.getId());
    int chunksNo = Utils.numberOfChunks(file.length());
    return restorers.computeIfAbsent(fileId, id -> new Restorer(pathname, id, chunksNo));
  }
}
