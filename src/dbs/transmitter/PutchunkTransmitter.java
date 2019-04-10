package dbs.transmitter;

import dbs.*;
import dbs.message.Message;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class PutchunkTransmitter implements Runnable {

  private Peer peer;
  private String pathname;
  private int replicationDegree;
  private String fileId;
  private long numberChunks;
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
      Peer.LOGGER.severe("Invalid path name.\n");
      return;
    }

    // hash the pathname
    this.fileId = null;
    try {
      this.fileId = Utils.hash(fileToBackup, peer.getId());
    } catch (Exception e) {
      Peer.LOGGER.severe("Could not retrieve a file id for the path name " + pathname + "\n");
      return;
    }

    this.numberChunks = (fileToBackup.getTotalSpace() / Protocol.chunkSize);

    if(this.backedUpFile()) {
      this.peer.LOGGER.info("1\n");
      Peer.LOGGER.info("Successfully backed up file '" + pathname + "'.\n");
      return;
    }
    else if (transmissionNumber == 6) {
      peer.LOGGER.severe("Could not backup file '" + pathname + "' after 5 attempts. Backup cancelled.\n");
      return;
    }
    else if (transmissionNumber == 1) {
      this.peer.fileInfoManager.setDesiredReplicationDegree(fileId, replicationDegree);
    }
    else
      peer.LOGGER.info("Could not backup file '" + pathname + "' on attempt number " + (transmissionNumber - 1) + ". Going to try again.\n");

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
      Peer.LOGGER.severe("Could not find the '" + fileToBackup + "' file.\n");
    }

    do {
      chunkNumber++;
      Integer chunkCurrentReplicationDegree = peer.fileInfoManager.getChunkReplicationDegree(fileId, chunkNumber);

      try {
        numberBytesRead = fis.read(chunk, 0, Protocol.chunkSize);
      } catch (IOException e) {
        Peer.LOGGER.severe("Could not read from the '" + fileToBackup + "' file.\n");
        break;
      }

      if (numberBytesRead > 0) {
        if (chunkCurrentReplicationDegree < replicationDegree)
          sendMessage(fileId, chunkNumber, chunk);
      }
      this.numberChunks = chunkNumber;
      chunk = new byte[Protocol.chunkSize];
    }
    while (numberBytesRead == Protocol.chunkSize);
  }

  private void sendMessage(String fileId, int chunkNumber, byte[] chunk) {
    Message putchunkMessage = Message.PUTCHUNK(fileId,
            peer.getConfig().version, chunkNumber, this.replicationDegree, chunk);
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
