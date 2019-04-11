package dbs.transmitter;

import dbs.*;
import dbs.fileInfoManager.FileInfoManager;
import dbs.message.Message;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
  private Future scheduled;
  private boolean done;

  /**
   * Construct a Chunker for a GETCHUNK message with this key.
   *
   * @param key The requested chunk identifier (and also the key in the chunkers map)
   */
  ChunkTransmitter(@NotNull ChunkKey key) {
    this.key = key;
    randomSchedule();
  }

  /**
   * Randomly schedule the CHUNK message to be sent in the near future.
   */
  private void randomSchedule() {
    int wait = Utils.getRandom(Protocol.minDelay, Protocol.maxDelay);
    this.scheduled = Peer.getInstance().getPool().schedule(this, wait,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Finish the Chunker, removing it from the map to release memory.
   * Called if the Chunker decided to respond with the requested chunk, regardless of
   * whether or not it succeeded.
   */
  private synchronized void end() {
    if (done) return;
    done = true;
    RestoreHandler.getInstance().chunkers.remove(key);
  }

  /**
   * Finish the Chunker, removing it from the map to release memory.
   * Called by the chunk receiver if it receives a chunk with this Chunker's key.
   */
  synchronized void detect() {
    if (done) return;
    done = true;
    RestoreHandler.getInstance().chunkers.remove(key);
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
    byte[] chunk = FileInfoManager.getInstance().getChunk(fileId, chunkNo);
    if (chunk == null) {
      end();
      return;
    }

    end();
    Message message = Message.CHUNK(fileId, Configuration.version, chunkNo, chunk);
    Peer.getInstance().send(message);
  }
}
