package dbs.transmitter;

import dbs.ChunkKey;
import dbs.Configuration;
import dbs.Peer;
import dbs.Protocol;
import dbs.files.FileInfoManager;
import dbs.message.Message;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public class Putchunker implements Runnable {

  private final ChunkKey key;
  private final Message message;
  private final int desiredReplicationDegree;
  private final byte[] chunk;
  private int attempts = 0;
  private Future task;
  private final AtomicBoolean done = new AtomicBoolean(false);

  Putchunker(ChunkKey key, int replication, byte[] chunk) {
    String fileId = key.getFileId();
    int chunkNo = key.getChunkNo();

    this.key = key;
    this.desiredReplicationDegree = replication;
    this.chunk = chunk;
    this.message = Message.PUTCHUNK(fileId, chunkNo, replication, chunk);
  }

  private void sleep() throws InterruptedException {
    if (done.get() || task == null) return;
    Thread.sleep(Protocol.delayPutchunker * (1 << attempts++));
  }

  void submit() {
    if (done.get() || task != null) return;
    task = BackupHandler.getInstance().putchunkPool.submit(this);
  }

  private boolean verify() {
    String fileId = key.getFileId();
    int no = key.getChunkNo();
    int perceived = FileInfoManager.getInstance().getChunkReplicationDegree(fileId, no);
    return perceived < desiredReplicationDegree;
  }

  private void fail() {
    if (done.getAndSet(true)) return;
    Peer.log("Failed to backup " + key + " with desired replication degree of "
        + desiredReplicationDegree, Level.WARNING);
    BackupHandler.getInstance().putchunkers.remove(key);
  }

  private void succeed() {
    if (done.getAndSet(true)) return;
    Peer.log("Successfully backed up " + key + " with desired replication degree.",
        Level.INFO);
    BackupHandler.getInstance().putchunkers.remove(key);
  }

  public ChunkKey getKey() {
    return key;
  }

  public byte[] getChunk() {
    return chunk;
  }

  public boolean isDone() {
    return done.get();
  }

  @Override
  public void run() {
    while (!done.get() && attempts <= Configuration.maxPutchunkAttempts) {
      Peer.getInstance().send(message);
      try {
        sleep();
      } catch (InterruptedException ignored) {
        Peer.log("Putchunker interrupted while sleeping", Level.WARNING);
      }
      if (verify()) {
        succeed();
        return;
      }
    }
    fail();
  }
}
