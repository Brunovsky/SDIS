package dbs.transmitter;

import dbs.ChunkKey;
import dbs.Peer;
import dbs.message.Message;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class DataRestoreTransmitter {
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

  private final class Chunker implements Runnable {
    private final ChunkKey key;
    private boolean detected;
    private boolean done;

    Chunker(@NotNull ChunkKey key) {
      this.key = key;
      randomSchedule();
    }

    public ChunkKey getKey() {
      return key;
    }

    private void randomSchedule() {
      // TODO: random delay
      peer.getPool().schedule(this, 400, TimeUnit.MILLISECONDS);
    }

    private synchronized void end() {
      chunkers.remove(key);
      done = true;
    }

    private synchronized void detect() {
      detected = false;
    }

    public synchronized boolean isDetected() {
      return detected;
    }

    public synchronized boolean isDone() {
      return done;
    }

    @Override
    public synchronized void run() {
      // is the chunk still expected?
      if (detected) {
        end();
        return;
      }

      // Get the chunk. Ensure we still have it and no unexpected IO error occurred.
      byte[] chunk = peer.fileInfoManager.getChunk();
      if (chunk == null) {
        end();
        return;
      }

      String fileId = key.getFileId();
      int chunkNo = key.getChunkNo();
      Message message = Message.CHUNK(fileId, peer.getConfig().version, chunkNo, chunk);

      peer.send(message);
      end();
    }
  }

  private final class Getchunker implements Runnable {
    private final Message message;
    private final ChunkKey key;
    private int attempts = 0;
    private byte[] chunk;
    private boolean received;
    private boolean done;

    Getchunker(@NotNull ChunkKey key) {
      String fileId = key.getFileId();
      int chunkNo = key.getChunkNo();

      this.key = key;
      this.message = Message.GETCHUNK(fileId, peer.getConfig().version, chunkNo);
      schedule();
    }

    public ChunkKey getKey() {
      return key;
    }

    private void schedule() {
      int wait = peer.getConfig().waitGetchunk * (1 << attempts);
      peer.getPool().schedule(this, wait, TimeUnit.MILLISECONDS);
    }

    private synchronized void end() {
      getchunkers.remove(key);
      done = true;
    }

    private synchronized void assign(byte[] receivedChunk) {
      if (received) return;
      received = true;
      chunk = receivedChunk;
    }

    public synchronized boolean isReceived() {
      return received;
    }

    public synchronized boolean isDone() {
      return done;
    }

    public synchronized byte[] getChunk() {
      return chunk;
    }

    @Override
    public synchronized void run() {
      // Has the chunk been received?
      if (received) {
        end();
        return;
      }
      // Did not receive the chunk.

      // Ran out of attempts. Quit (done status) without having received the chunk.
      if (attempts++ >= peer.getConfig().maxGetchunkAttempts) {
        end();
        return;
      }

      // Try again
      peer.send(message);
      schedule();
    }
  }

  public DataRestoreTransmitter(@NotNull Peer peer) {
    this.peer = peer;
    this.chunkers = new ConcurrentHashMap<>();
    this.getchunkers = new ConcurrentHashMap<>();
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

    // Exit immediately if we don't have the chunk.
    if (!peer.fileInfoManager.hasChunk(fileId, chunkNo)) return null;

    ChunkKey key = new ChunkKey(fileId, chunkNo);
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

    // Update Getchunker
    Getchunker getchunker = getchunkers.get(key);
    if (getchunker != null) getchunker.assign(bytes);

    // Update Chunker
    Chunker chunker = chunkers.get(key);
    if (chunker != null) chunker.detect();
  }

  /**
   * Called by the RESTORE initiator to create a Getchunker for a given chunk.
   * If we have a running Getchunker for this chunk we return it; otherwise we create a
   * new one for it and return it.
   *
   * @param fileId The id of the file being restored
   * @param chunkNo The number of the chunk being restored
   * @return The Getchunker instance
   */
  public Getchunker sendGETCHUNK(@NotNull String fileId, int chunkNo) {
    ChunkKey key = new ChunkKey(fileId, chunkNo);
    return getchunkers.computeIfAbsent(key, Getchunker::new);
  }
}
