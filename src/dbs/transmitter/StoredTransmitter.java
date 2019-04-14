package dbs.transmitter;

import dbs.Peer;
import dbs.Protocol;
import dbs.Utils;
import dbs.message.Message;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class StoredTransmitter implements Runnable {

  private String fileId;
  private Future scheduled;
  private Integer chunkNumber;

  public StoredTransmitter(String fileId, Integer chunkNumber) {
    this.fileId = fileId;
    this.chunkNumber = chunkNumber;
    int delay = Utils.getRandom(Protocol.minDelay, Protocol.maxDelay);
    this.scheduled = BackupHandler.getInstance().storedPool.schedule(this, delay,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void run() {
    // TODO: Enhancement PUTCHUNK: no-store/[no-response] logic
    Message message = Message.STORED(this.fileId, this.chunkNumber);
    Peer.getInstance().send(message);
  }
}
