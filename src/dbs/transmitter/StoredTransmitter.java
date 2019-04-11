package dbs.transmitter;

import dbs.Peer;
import dbs.message.Message;

public class StoredTransmitter implements Runnable {

  private Peer peer;
  String version;
  private String fileId;
  private Integer chunkNumber;

  public StoredTransmitter(String version, Peer peer, String fileId, Integer chunkNumber) {
    this.version = version;
    this.peer = peer;
    this.fileId = fileId;
    this.chunkNumber = chunkNumber;
  }

  @Override
  public void run() {
    Message message = Message.STORED(this.fileId, this.version, this.chunkNumber);
    message.setSenderId(Long.toString(this.peer.getId()));
    this.peer.send(message);
  }
}
