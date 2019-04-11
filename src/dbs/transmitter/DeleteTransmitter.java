package dbs.transmitter;

import dbs.Configuration;
import dbs.Peer;
import dbs.Protocol;
import dbs.fileInfoManager.FileInfoManager;
import dbs.files.FileRequest;
import dbs.files.FilesManager;
import dbs.message.Message;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class DeleteTransmitter implements Runnable {

  private String pathname;
  private String fileId;
  private int transmissionNumber;
  private boolean runEnhancedVersion;

  public DeleteTransmitter(String pathname, int transmissionNumber, boolean runEnhancedVersion) {
    this.pathname = pathname;
    this.transmissionNumber = transmissionNumber;
    this.runEnhancedVersion = runEnhancedVersion;
  }

  public DeleteTransmitter(String pathname, int transmissionNumber, String fileId, boolean runEnhancedVersion) {
    this(pathname, transmissionNumber,runEnhancedVersion);
    this.fileId = fileId;
  }

  @Override
  public void run() {
    if(this.runEnhancedVersion)
      this.runEnhanced();
    else
      this.runNotEnhanced();
  }

  public void runNotEnhanced() {

    if(this.transmissionNumber == 1) {

      // check if the given pathname is valid
      File fileToDelete;
      FileRequest fileRequest = FilesManager.retrieveFileInfo(pathname, Peer.getInstance().getId());
      if (fileRequest == null) {
        Peer.log("Could not access the provided file (" + this.pathname + ") for deletion.", Level.SEVERE);
        return;
      } else {
        fileToDelete = fileRequest.getFile();
        this.fileId = fileRequest.getFileId();
      }

      // delete file
      if (!FileInfoManager.getInstance().deleteFile(fileToDelete, this.fileId)) {
        Peer.log("Could not perform the deletion of the file " + this.pathname, Level.SEVERE);
        return;
      }
    }

    // send DELETE message
    Peer.log("Going to send the delete message for the file with id " + this.fileId + " (transmission number " + this.transmissionNumber + ")", Level.INFO);
    Peer.getInstance().send(Message.DELETE(this.fileId, Configuration.version));

    if(this.transmissionNumber != Protocol.numberDeleteMessages) {
      // schedule next transmission of the delete message
      Peer.getInstance().getPool().schedule(new DeleteTransmitter(this.pathname, ++transmissionNumber, this.fileId, this.runEnhancedVersion),
              Protocol.delaySendDelete,
              TimeUnit.SECONDS);
    }
    else
      Peer.log("Sent all DELETE messages (" + Protocol.numberDeleteMessages + " messages) for the file with id " + fileId, Level.INFO);
  }

  public void runEnhanced() {

  }
}
