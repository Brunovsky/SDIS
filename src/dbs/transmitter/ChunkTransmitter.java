package dbs.transmitter;

import dbs.*;
import dbs.files.FilesManager;
import dbs.message.Message;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles the response to a GETCHUNK message, as it must consider the possibility of
 * responding with a CHUNK message within a small window of time. The Chunker
 * registers himself in the chunkers map, to be alerted by the chunk receiver if
 * another CHUNK message of the same type is detected on the multicast network,
 * and promptly cancelled.
 * We provide Future-like functionality with isDone(), but this should not be used.
 * TODO: A Chunker instance is public only for testing purposes.
 */
public class ChunkTransmitter implements Runnable {

  private final ChunkKey key;
  private Future task;
  private final AtomicBoolean done = new AtomicBoolean(false);

  /**
   * Construct a Chunker for a GETCHUNK message with this key.
   *
   * @param key The requested chunk identifier (and also the key in the chunkers map)
   */
  ChunkTransmitter(ChunkKey key) {
    this.key = key;
    int wait = Utils.getRandom(Protocol.minDelay, Protocol.maxDelay);
    task = RestoreHandler.getInstance().chunkPool.schedule(this, wait,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Finish the Chunker, removing it from the map to release memory.
   * Called if the Chunker decided to respond with the requested chunk, regardless of
   * whether or not it succeeded.
   * Called by the owner thread.
   */
  private void end() {
    if (done.getAndSet(true)) return;
    RestoreHandler.getInstance().chunkers.remove(key);
  }

  /**
   * Finish the Chunker, removing it from the map to release memory.
   * Called by the chunk receiver if it receives a chunk with this Chunker's key.
   * Called by the chunk receiver.
   */
  void detect() {
    if (done.getAndSet(true)) return;
    RestoreHandler.getInstance().chunkers.remove(key);
    task.cancel(true);
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
  public boolean isDone() {
    return done.get();
  }

  /**
   * Scheduled function, run when the Chunker is not aborted by the chunk receiver
   * and it decides to send the message.
   */
  @Override
  public void run() {
    if (done.get()) return;
    end();

    String fileId = key.getFileId();
    int chunkNo = key.getChunkNo();

    // Get the chunk. Ensure we still have it and no unexpected IO error occurred.
    byte[] chunk = FilesManager.getInstance().getChunk(fileId, chunkNo);
    if (chunk == null) return;

    Message message = Message.CHUNK(fileId, Configuration.version, chunkNo, chunk);
    Peer.getInstance().send(message);
  }
}
