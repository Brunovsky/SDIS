package dbs.transmitter;

import dbs.*;
import dbs.fileInfoManager.FileInfoManager;
import dbs.files.FileRequest;
import dbs.files.FilesManager;
import dbs.message.Message;
import dbs.message.MessageError;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class PutchunkTransmitter implements Runnable {

  private String pathname;
  private int replicationDegree;
  private String fileId;
  private Integer numberChunks;
  private int transmissionNumber;

  public PutchunkTransmitter(String pathname, int replicationDegree,
                             int transmissionNumber) {
    this.pathname = pathname;
    this.replicationDegree = replicationDegree;
    this.transmissionNumber = transmissionNumber;
  }

  @Override
  public void run() {

    File fileToBackup;

    FileRequest fileRequest = FilesManager.retrieveFileInfo(pathname,
        Peer.getInstance().getId());
    if (fileRequest == null) {
      Peer.log("Could not access the provided file", Level.SEVERE);
      return;
    } else {
      fileToBackup = fileRequest.getFile();
      this.fileId = fileRequest.getFileId();
      this.numberChunks = fileRequest.getNumberChunks();
    }

    if (this.backedUpFile()) {
      Peer.log("Successfully backed up file '" + pathname, Level.INFO);
      return;
    } else if (transmissionNumber == 6) {
      Peer.log("Could not backup file '" + pathname + "' after 5 attempts. Backup " +
          "cancelled", Level.SEVERE);
      return;
    } else if (transmissionNumber == 1) {
      FileInfoManager.getInstance().setDesiredReplicationDegree(fileId,
          replicationDegree);
    } else
      Peer.log("Could not backup file '" + pathname + "' on attempt number " + (transmissionNumber - 1) + ". Going to try again", Level.WARNING);

    // send PUTCHUNK messages
    this.transmitFile(fileToBackup);

    // schedule next data backup transmitter thread
    Peer.log("waiting: " + (int)Math.pow(2, this.transmissionNumber - 1), Level.INFO);
    Peer.getInstance().getPool().schedule(new PutchunkTransmitter(pathname,
            replicationDegree, ++transmissionNumber), (int)Math.pow(2, this.transmissionNumber - 1),
        TimeUnit.SECONDS);
  }

  private void transmitFile(File fileToBackup) {

    byte[] chunk = new byte[Protocol.chunkSize];
    int numberBytesRead = 0;
    int chunkNumber = 0;

    FileInputStream fis = null;
    try {
      fis = new FileInputStream(fileToBackup);
    } catch (FileNotFoundException e) {
      Peer.log("Could not find the '" + fileToBackup + "' file", Level.SEVERE);
    }

    do {
      chunkNumber++;
      Integer chunkCurrentReplicationDegree =
          FileInfoManager.getInstance().getChunkReplicationDegree(fileId, chunkNumber);

      try {
        numberBytesRead = fis.read(chunk, 0, Protocol.chunkSize);
      } catch (IOException e) {
        Peer.log("Could not read from the '" + fileToBackup + "' file", Level.SEVERE);
        break;
      }

      if (numberBytesRead > 0) {
        if (chunkCurrentReplicationDegree < replicationDegree)
          sendMessage(fileId, chunkNumber, chunk);
      }
      chunk = new byte[Protocol.chunkSize];
    }
    while (numberBytesRead == Protocol.chunkSize);
  }

  private void sendMessage(String fileId, int chunkNumber, byte[] chunk) {
    Message putchunkMessage = null;
    try {
      Peer.log("Sending PUTCHUNK for chunk " + chunkNumber, Level.INFO);
      putchunkMessage = Message.PUTCHUNK(fileId,
          Configuration.version, chunkNumber, this.replicationDegree, chunk);
    } catch (MessageError e) {
      Peer.log("Could not generate PUTCHUNK message. Going to abort execution of the " +
          "PUTCHUNK protocol", Level.SEVERE);
    }
    Peer.getInstance().send(putchunkMessage);
  }

  private boolean backedUpFile() {
    for (int chunkNumber = 1; chunkNumber <= this.numberChunks; chunkNumber++) {
      Integer actualReplicationDegree =
          FileInfoManager.getInstance().getChunkReplicationDegree(fileId,
              chunkNumber);
      if (actualReplicationDegree < replicationDegree) {
        return false;
      }
    }
    return true;
  }
}
