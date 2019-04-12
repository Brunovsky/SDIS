package dbs.transmitter;


import dbs.ChunkKey;
import dbs.Configuration;
import dbs.Peer;
import dbs.Protocol;
import dbs.message.Message;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles one instance of the GETCHUNK protocol initiated by this peer. The
 * GetchunkTransmitter starts by sending a GETCHUNK message to the multicast network, and
 * awaits its response in the form of an alert (assign) from the chunk receiver. If no
 * alert comes, it tries again, up to a maximum number of times before aborting.
 * TODO: A Getchunker instance is public only for testing purposes.
 */
public class GetchunkTransmitter implements Runnable {

  private final ChunkKey key;
  private final Restorer restorer;
  private final Message message;
  private volatile byte[] chunk;
  private int attempts = 0;
  private Future task;
  private AtomicBoolean done = new AtomicBoolean(false);

  /**
   * Construct a Getchunker for this chunk key.
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
  private void sleep() throws InterruptedException {
    if (done.get() || task == null) return;
    Thread.sleep(Protocol.delayGetchunker * (1 << attempts++));
  }

  /**
   * Submit the Getchunker to the thread pool.
   */
  void submit() {
    if (done.get() || task != null) return;
    task = RestoreHandler.getInstance().getchunkPool.submit(this);
  }

  /**
   * Terminate this Getchunker unsuccessfully because the number of retries ran
   * out. Alert the parent Restorer to this fact, so it can cancel the other Getchunkers
   * of this file.
   * Called by the owner thread.
   */
  private void fail() {
    if (done.getAndSet(true)) return;
    RestoreHandler.getInstance().getchunkers.remove(key);
    restorer.failed(key);
  }

  /**
   * Terminate this Getchunker unsuccessfully because another Getchunker
   * for the same file ran out of retries.
   * Called by another Getchunk thread, from Restorer.failed().
   */
  void cancel() {
    if (done.getAndSet(true)) return;
    RestoreHandler.getInstance().getchunkers.remove(key);
    if (task != null) task.cancel(true);
  }

  /**
   * Terminate this Getchunker successfully, assigning the received chunk to the
   * promise, and alert the parent Restorer to the conclusion.
   * Called by the chunk receiver.
   *
   * @param received The chunk detected by the chunk receiver
   */
  void assign(byte @NotNull [] received) {
    if (done.getAndSet(true)) return;
    RestoreHandler.getInstance().getchunkers.remove(key);
    if (task != null) task.cancel(true);
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
    return done.get();
  }

  /**
   * Scheduled function, run when the Getchunker is not aborted by the chunk
   * receiver
   * and it decides to resend the GETCHUNK message. It may run out of retries.
   */
  @Override
  public void run() {
    while (!done.get() && attempts <= Configuration.maxGetchunkAttempts) {
      Peer.getInstance().send(message);
      try {
        sleep();
      } catch (InterruptedException e) {
        fail();
        return;
      }
    }
    fail();
  }
}