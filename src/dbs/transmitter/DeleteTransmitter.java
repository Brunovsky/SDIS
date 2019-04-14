package dbs.transmitter;

import dbs.Configuration;
import dbs.Peer;
import dbs.Protocol;
import dbs.files.FileInfoManager;
import dbs.files.OwnFileInfo;
import dbs.message.Message;

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

  private boolean deleteFile() {
    if(this.transmissionNumber != 1) return true;
    // check if the given pathname is valid
    OwnFileInfo info = FileInfoManager.getInstance().getPathname(pathname);
    if (info == null) {
      Peer.log("This peer does not have a record of file " + pathname, Level.WARNING);
      return false;
    } else {
      this.fileId = info.getFileId();
    }

    if(!this.runEnhancedVersion)
      FileInfoManager.getInstance().deleteOwnFileInfo(this.fileId);

    return true;
  }

  private void runNotEnhanced() {

    if (!this.deleteFile()) return;

    // send DELETE message
    this.sendDeleteMessage();

    if(this.transmissionNumber != Protocol.numberDeleteMessages) {
      // schedule next transmission of the delete message
      Peer.getInstance().getPool().schedule(new DeleteTransmitter(this.pathname, ++transmissionNumber, this.fileId, this.runEnhancedVersion),
              Protocol.delaySendDelete,
              TimeUnit.SECONDS);
    }
    else
      Peer.log("Sent all DELETE messages (" + Protocol.numberDeleteMessages + " messages) for the file with id " + fileId, Level.INFO);
  }

  private void sendDeleteMessage() {
    Peer.log("Going to send the delete message for the file with id " + this.fileId + " (transmission number " + this.transmissionNumber + ")", Level.INFO);
    Peer.getInstance().send(Message.DELETE(this.fileId, Configuration.version));
  }

  private void scheduleNextDeleteMessage() {
    Peer.getInstance().getPool().schedule(new DeleteTransmitter(this.pathname, ++transmissionNumber, this.fileId, this.runEnhancedVersion),
            (int)Math.pow(2, this.transmissionNumber - 1),
            TimeUnit.SECONDS);
  }


  private void runEnhanced() {
    this.deleteFile();
    if(FileInfoManager.getInstance().hasBackupPeers(this.fileId))
    {
      Peer.log("Not all peers have deleted the file with id " + fileId + ". Sending delete message (attempt " + this.transmissionNumber + ")", Level.INFO);
      this.sendDeleteMessage();
      this.scheduleNextDeleteMessage();
    }
    else {
      Peer.log("All peers have deleted the file with id " + fileId, Level.INFO);
      FileInfoManager.getInstance().deleteOwnFileInfo(this.fileId);
    }
  }
}
