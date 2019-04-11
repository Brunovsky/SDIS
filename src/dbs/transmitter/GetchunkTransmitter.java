package dbs.transmitter;


import dbs.ChunkKey;
import dbs.Configuration;
import dbs.Peer;
import dbs.Protocol;
import dbs.message.Message;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Handles one instance of the GETCHUNK protocol initiated by this peer. The
 * GetchunkTransmitter starts by sending a GETCHUNK message to the multicast network, and
 * awaits its response in the form of an alert (assign) from the chunk receiver. If no
 * alert comes, it tries again, up to a maximum number of times before aborting.
 * TODO: A GetchunkTransmitter instance is public only for testing purposes.
 */
public class GetchunkTransmitter implements Runnable {

  private final ChunkKey key;
  private final Restorer restorer;
  private final Message message;
  private volatile byte[] chunk;
  private int attempts = 0;
  private Future scheduled;
  private volatile boolean done;

  /**
   * Construct a GetchunkTransmitter for this chunk key.
   *
   * @param key The request chunk's key (and also the key in the getchunkers map)
   */
  GetchunkTransmitter(@NotNull ChunkKey key, @NotNull Restorer restorer) {
    String fileId = key.getFileId();
    int chunkNo = key.getChunkNo();

    this.key = key;
    this.restorer = restorer;
    this.message = Message.GETCHUNK(fileId, Configuration.version, chunkNo);
  }

  /**
   * Schedule the GETCHUNK message to be resent in the near future.
   */
  private synchronized void schedule() {
    if (done) return;
    int time = Protocol.delayGetchunker * (1 << attempts);
    scheduled = Peer.getInstance().getPool().schedule(this, time, TimeUnit.MILLISECONDS);
  }

  /**
   * Submit the GetchunkTransmitter to the thread pool.
   */
  synchronized void submit() {
    if (done || scheduled != null) return;
    scheduled = Peer.getInstance().getPool().submit(this);
  }

  /**
   * Terminate this GetchunkTransmitter unsuccessfully because the number of retries ran
   * out.
   * Alert the parent Restorer to this fact, so it can cancel the other Getchunkers
   * of this file.
   */
  private synchronized void end() {
    if (done) return;
    done = true;
    RestoreHandler.getInstance().getchunkers.remove(key);
    restorer.failed(key);
  }

  /**
   * Terminate this GetchunkTransmitter unsuccessfully because another
   * GetchunkTransmitter
   * for
   * the
   * same
   * file ran out of retries. This is called by the parent Restorer.
   */
  synchronized void cancel() {
    if (done) return;
    done = true;
    RestoreHandler.getInstance().getchunkers.remove(key);
    if (scheduled != null) scheduled.cancel(false);
  }

  /**
   * Terminate this GetchunkTransmitter successfully, assigning the received chunk to the
   * promise, and alert the parent Restorer to the conclusion.
   *
   * @param received The chunk detected by the chunk receiver
   */
  synchronized void assign(byte @NotNull [] received) {
    if (done) return;
    done = true;
    RestoreHandler.getInstance().getchunkers.remove(key);
    if (scheduled != null) scheduled.cancel(false);
    chunk = received;
    restorer.assigned(key);
  }

  /**
   * @return This GetchunkTransmitter's key
   */
  public ChunkKey getKey() {
    return key;
  }

  /**
   * @return This GetchunkTransmitter's retrieved chunk
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
   * Scheduled function, run when the GetchunkTransmitter is not aborted by the chunk
   * receiver
   * and it decides to resend the GETCHUNK message. It may run out of retries.
   */
  @Override
  public synchronized void run() {
    if (done) return;

    // Ran out of attempts. Quit (done status) without having received the chunk.
    if (attempts++ > Configuration.maxGetchunkAttempts) {
      end();
      return;
    }

    // Try again
    Peer.getInstance().send(message);
    schedule();
  }
}