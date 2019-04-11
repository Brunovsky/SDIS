package dbs.transmitter;

import dbs.Peer;
import dbs.Protocol;
import dbs.files.FileRequest;
import dbs.files.FilesManager;
import dbs.message.Message;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class DeleteTransmitter implements Runnable {

  private Peer peer;
  private String pathname;
  private String fileId;
  private int transmissionNumber;

  public DeleteTransmitter(Peer peer, String pathname, int transmissionNumber) {
    this.peer = peer;
    this.pathname = pathname;
    this.transmissionNumber = transmissionNumber;
  }

  public DeleteTransmitter(Peer peer, String pathname, int transmissionNumber, String fileId) {
    this(peer, pathname, transmissionNumber);
    this.fileId = fileId;
  }


  @Override
  public void run() {

    if(this.transmissionNumber == 1) {

      // check if the given pathname is valid
      File fileToDelete = null;
      FileRequest fileRequest = FilesManager.retrieveFileInfo(pathname, this.peer.getId());
      if (fileRequest == null) {
        Peer.log("Could not access the provided file", Level.SEVERE);
        return;
      } else {
        fileToDelete = fileRequest.getFile();
        this.fileId = fileRequest.getFileId();
      }

      // delete file
      if (!this.peer.fileInfoManager.deleteFile(fileToDelete, this.fileId)) {
        Peer.log("Could not perform the deletion of the file " + this.pathname, Level.SEVERE);
        return;
      }
    }

    // send DELETE message
    Peer.log("Going to send the delete message for the file with id " + this.fileId + " (transmission number " + this.transmissionNumber + ")", Level.INFO);
    this.peer.send(Message.DELETE(this.fileId, peer.getConfig().version));

    if(this.transmissionNumber != Protocol.numberDeleteMessages) {
      // schedule next transmission of the delete message
      this.peer.getPool().schedule(new DeleteTransmitter(this.peer, this.pathname, ++transmissionNumber, this.fileId),
              Protocol.delaySendDelete,
              TimeUnit.SECONDS);
    }
    else
      Peer.log("Sent all DELETE messages (" + Protocol.numberDeleteMessages + " messages) for the file with id " + fileId, Level.INFO);
  }
}
