package dbs.transmitter;

import dbs.*;
import dbs.message.Message;
import dbs.message.MessageError;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class PutchunkTransmitter implements Runnable {

  private Peer peer;
  private String pathname;
  private int replicationDegree;
  private String fileId;
  private int numberChunks;
  private int transmissionNumber;

  public PutchunkTransmitter(Peer peer, String pathname, int replicationDegree, int transmissionNumber) {
    this.peer = peer;
    this.pathname = pathname;
    this.replicationDegree = replicationDegree;
    this.transmissionNumber = transmissionNumber;
  }

  @Override
  public void run() {

    File fileToBackup = new File(pathname);

    // check if path name corresponds to a valid file
    if (!fileToBackup.exists() || fileToBackup.isDirectory()) {
      Peer.log("Invalid path name", Level.SEVERE);
      return;
    }

    // hash the pathname
    this.fileId = null;
    try {
      this.fileId = Utils.hash(fileToBackup, peer.getId());
    } catch (Exception e) {
      Peer.log("Could not retrieve a file id for the path name " + pathname, Level.SEVERE);
      return;
    }

    Long fileSize = fileToBackup.length();
    this.numberChunks = (int)Math.ceil(fileSize / (double)Protocol.chunkSize);

    if(this.backedUpFile()) {
      Peer.log("Successfully backed up file '" + pathname, Level.INFO);
      return;
    }
    else if (transmissionNumber == 6) {
      Peer.log("Could not backup file '" + pathname + "' after 5 attempts. Backup cancelled", Level.SEVERE);
      return;
    }
    else if (transmissionNumber == 1) {
      this.peer.fileInfoManager.setDesiredReplicationDegree(fileId, replicationDegree);
    }
    else
      Peer.log("Could not backup file '" + pathname + "' on attempt number " + (transmissionNumber - 1) + ". Going to try again", Level.WARNING);

    // send PUTCHUNK messages
    this.transmitFile(fileToBackup);

    // schedule next data backup transmitter thread
    this.peer.getPool().schedule(new PutchunkTransmitter(this.peer, pathname, replicationDegree, ++transmissionNumber),
            Protocol.delayReceiveStored * this.transmissionNumber,
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
      Integer chunkCurrentReplicationDegree = peer.fileInfoManager.getChunkReplicationDegree(fileId, chunkNumber);

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
      putchunkMessage = Message.PUTCHUNK(fileId,
              peer.getConfig().version, chunkNumber, this.replicationDegree, chunk);
    } catch (MessageError e) {
      Peer.log("Could not generate PUTCHUNK message. Going to abort execution of the PUTCHUNK protocol", Level.SEVERE);
    }
    peer.send(putchunkMessage);
  }

  private boolean backedUpFile() {
    for (int chunkNumber = 1; chunkNumber <= this.numberChunks; chunkNumber++) {
      Integer actualReplicationDegree = peer.fileInfoManager.getChunkReplicationDegree(fileId, chunkNumber);
      if (actualReplicationDegree < replicationDegree) {
        return false;
      }
    }
    return true;
  }
}
