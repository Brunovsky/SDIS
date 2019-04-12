package dbs.transmitter;

import dbs.Peer;
import dbs.message.Message;

public class StoredTransmitter implements Runnable {

  private String version;
  private String fileId;
  private Integer chunkNumber;

  public StoredTransmitter(String version, String fileId, Integer chunkNumber) {
    this.version = version;
    this.fileId = fileId;
    this.chunkNumber = chunkNumber;
  }

  @Override
  public void run() {
    Message message = Message.STORED(this.fileId, this.version, this.chunkNumber);
    Peer.getInstance().send(message);
  }
}
