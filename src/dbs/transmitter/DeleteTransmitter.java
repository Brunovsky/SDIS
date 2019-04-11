package dbs.transmitter;

import dbs.Peer;
import dbs.files.FileRequest;
import dbs.files.FilesManager;
import dbs.message.Message;

import java.io.File;
import java.util.logging.Level;

public class DeleteTransmitter implements Runnable {

  private Peer peer;
  private String pathname;
  private String fileId;

  public DeleteTransmitter(Peer peer, String pathname) {
    this.peer = peer;
    this.pathname = pathname;
  }


  @Override
  public void run() {

    // check if the given pathname is valid
    File fileToDelete= null;
    FileRequest fileRequest = FilesManager.retrieveFileInfo(pathname, this.peer.getId());
    if(fileRequest == null) {
      Peer.log("Could not access the provided file", Level.SEVERE);
      return;
    }
    else {
      fileToDelete = fileRequest.getFile();
      this.fileId = fileRequest.getFileId();
    }

    // delete file
    if(!this.peer.fileInfoManager.deleteFile(fileToDelete, this.fileId)) {
      Peer.log("Could not perform the deletion of the file " + this.pathname, Level.SEVERE);
      return;
    }

    // send DELETE message
    this.peer.send(Message.DELETE(this.fileId, peer.getConfig().version));
    Peer.log("sent message delete", Level.INFO);
  }
}
