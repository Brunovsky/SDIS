package dbs.transmitter;

import dbs.ChunkKey;
import dbs.Configuration;
import dbs.Peer;
import dbs.Utils;
import dbs.message.Message;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

public class DataBackupTransmitter implements Runnable {

  Peer peer;
  String pathname;
  int replicationDegree;
  byte[] fileId;
  int numberChunks;

  public DataBackupTransmitter(Peer peer, String pathname, int replicationDegree) {
    this.peer = peer;
    this.pathname = pathname;
    this.replicationDegree = replicationDegree;
  }

  @Override
  public void run() {
    File fileToBackup = new File(pathname);

    // check if path name corresponds to a valid file
    if (!fileToBackup.exists() || fileToBackup.isDirectory()) {
      peer.LOGGER.severe("Invalid path name.\n");
      System.exit(1);
    }

    // hash the pathname
    this.fileId = null;
    try {
      this.fileId = Utils.hash(fileToBackup, peer.getId());
    } catch (Exception e) {
      peer.LOGGER.severe("Could not retrieve a file id for the path name " + pathname + "\n");
      System.exit(1);
    }

    // send PUTCHUNK messages
    this.transmitFile(fileToBackup);

    // wait for the reception of confirmation messages
    for(int attemptNumber = 2; attemptNumber <= 5 ; attemptNumber++) {
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        peer.LOGGER.severe("Could not wait for the reception of STORED messages");
        System.exit(1);
      }
      if(backedUpFile()) {
        peer.LOGGER.info("Successfully backed up file '" + pathname + "'.");
        System.exit(0);
      }
      else {
        peer.LOGGER.warning("Could not back up file '" + pathname + "'. Going to try again (attempt number " + attemptNumber + ").");
        this.transmitFile(fileToBackup);
      }
    }
    peer.LOGGER.severe("Could not backup file '" + pathname + "'. Backup cancelled.");
    System.exit(1);
  }

  private void transmitFile(File fileToBackup) {

    byte[] chunk = new byte[Configuration.chunkSize];
    int numberBytesRead = 0;
    int chunkNumber = 0;

    FileInputStream fis = null;
    try{
      fis = new FileInputStream(fileToBackup);
    } catch (FileNotFoundException e) {
      peer.LOGGER.severe("Could not find the '"  + fileToBackup + "' file.");
    }

    do {
      chunkNumber++;
      ChunkKey chunkKey = new ChunkKey(this.fileId, chunkNumber);

      try {
        numberBytesRead = fis.read(chunk, 0, Configuration.chunkSize);
      } catch(IOException e) {
        peer.LOGGER.severe("Could not read from the '" + fileToBackup + "' file.");
        break;
      }

      if(numberBytesRead > 0) {

        if(peer.chunksReplicationDegree.containsKey(chunkKey)) {  // chunk already backed up
          Vector<Long> chunkPeers = peer.chunksReplicationDegree.get(chunkKey);
          if(chunkPeers.size() >= this.replicationDegree) // chunk's replication degree already set to the desired value
            break;
        }
        sendMessage(fileId, chunkNumber, chunk);
        this.numberChunks = chunkNumber;
        chunk = new byte[Configuration.chunkSize];
      }
    }
    while(numberBytesRead == Configuration.chunkSize);
  }

  private void sendMessage(byte[] fileId, int chunkNumber, byte[] chunk) {
    Message putchunkMessage = Message.PUTCHUNK(new String(fileId), peer.getProtocolVersion(), chunkNumber, this.replicationDegree, chunk);
    peer.send(putchunkMessage);
  }

  private boolean backedUpFile() {
    for (int chunckNumber = 1; chunckNumber <= this.numberChunks; chunckNumber++) {
      ChunkKey chunkKey = new ChunkKey(this.fileId, chunckNumber);
      if (peer.chunksReplicationDegree.containsKey(chunkKey)) {  // chunk already backed up
        Vector<Long> chunkPeers = peer.chunksReplicationDegree.get(chunkKey);
        if (chunkPeers.size() < this.replicationDegree) // chunk's replication degree already set to the desired value
          return false;
      } else
        return false;
    }
    return true;
  }
}
