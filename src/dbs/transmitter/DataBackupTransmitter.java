package dbs.transmitter;

import dbs.*;
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
  String fileId;
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

    // send PUTCHUNK messages
    this.transmitFile(fileToBackup);

    // wait for the reception of confirmation messages
    for(int attemptNumber = 2; attemptNumber <= 5 ; attemptNumber++) {
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        Peer.LOGGER.severe("Could not wait for the reception of STORED messages.\n");
        return;
      }
      if(backedUpFile()) {
        Peer.LOGGER.info("Successfully backed up file '" + pathname + "'.\n");
        return;
      }
      else {
        Peer.LOGGER.warning("Could not back up file '" + pathname + "'. Going to try " +
            "again (attempt number " + attemptNumber + ").\n");
        this.transmitFile(fileToBackup);
      }
    }
    Peer.LOGGER.severe("Could not backup file '" + pathname + "'. Backup cancelled.\n");
  }

  private void transmitFile(File fileToBackup) {

    byte[] chunk = new byte[Protocol.chunkSize];
    int numberBytesRead = 0;
    int chunkNumber = 0;

    FileInputStream fis = null;
    try{
      fis = new FileInputStream(fileToBackup);
    } catch (FileNotFoundException e) {
      Peer.LOGGER.severe("Could not find the '"  + fileToBackup + "' file.\n");
    }

    do {
      chunkNumber++;
      ChunkKey chunkKey = new ChunkKey(this.fileId, chunkNumber);

      try {
        numberBytesRead = fis.read(chunk, 0, Protocol.chunkSize);
      } catch(IOException e) {
        Peer.LOGGER.severe("Could not read from the '" + fileToBackup + "' file.\n");
        break;
      }

      if(numberBytesRead > 0) {

        if(peer.chunksReplicationDegree.containsKey(chunkKey)) {  // chunk already backed up
          Vector<Long> chunkPeers = peer.chunksReplicationDegree.get(chunkKey);
          if(chunkPeers.size() >= this.replicationDegree) // chunk's replication degree already set to the desired value
            break;
        }
        sendMessage(fileId.getBytes(), chunkNumber, chunk);
        this.numberChunks = chunkNumber;
        chunk = new byte[Protocol.chunkSize];
      }
    }
    while(numberBytesRead == Protocol.chunkSize);
  }

  private void sendMessage(String fileId, int chunkNumber, byte[] chunk) {
    Message putchunkMessage = Message.PUTCHUNK(fileId,
        peer.getConfig().version, chunkNumber, this.replicationDegree, chunk);
    peer.send(putchunkMessage);
  }

  private boolean backedUpFile() {
    for (int chunkNumber = 1; chunkNumber <= this.numberChunks; chunkNumber++) {
      ChunkKey chunkKey = new ChunkKey(this.fileId, chunkNumber);
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
