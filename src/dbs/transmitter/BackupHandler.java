package dbs.transmitter;

import dbs.*;
import dbs.files.FileInfoManager;
import dbs.files.OwnFileInfo;
import dbs.message.Message;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Level;

public class BackupHandler {

  private static BackupHandler handler;

  public static BackupHandler getInstance() {
    assert handler != null;
    return handler;
  }

  public static BackupHandler createInstance() {
    return handler == null ? (handler = new BackupHandler()) : handler;
  }

  final ConcurrentHashMap<ChunkKey,Putchunker> putchunkers;

  final ConcurrentHashMap<ChunkKey,StoredTransmitter> storers;

  final ScheduledThreadPoolExecutor putchunkPool;

  final ScheduledThreadPoolExecutor storedPool;

  private BackupHandler() {
    this.putchunkers = new ConcurrentHashMap<>();
    this.storers = new ConcurrentHashMap<>();
    this.putchunkPool = new ScheduledThreadPoolExecutor(Configuration.putchunkPoolSize);
    this.storedPool = new ScheduledThreadPoolExecutor(Configuration.storedPoolSize);
  }

  // Moved from ControlProcessor
  public void receivePUTCHUNK(Message message) {
    // Alert a waiting RemovedWaiter that a PUTCHUNK message arrived.
    ReclaimHandler.getInstance().alertPUTCHUNK(message);

    String fileId = message.getFileId();
    int chunkNumber = message.getChunkNo();
    int desiredReplicationDegree = message.getReplication();
    byte[] chunk = message.getBody();
    ChunkKey key = new ChunkKey(fileId, chunkNumber);

    // TODO: Backup memory used too high => no-store and no-response logic goes here
    // ...
    // TODO: proceed iff we decide to store the chunk.

    // TODO: Enhancement PUTCHUNK: [no-store]/no-response logic
    FileInfoManager.getInstance().storeChunk(fileId, chunkNumber, chunk);
    // ^^^ this adds us as backup peers
    FileInfoManager.getInstance().setDesiredReplicationDegree(fileId,
        desiredReplicationDegree);

    storers.computeIfAbsent(key, StoredTransmitter::new);
  }

  public void receiveSTORED(Message message) {
    String fileId = message.getFileId();
    int chunkNo = message.getChunkNo();
    Long senderId = Long.parseLong(message.getSenderId());

    FileInfoManager.getInstance().addBackupPeer(fileId, chunkNo, senderId);
  }

  public void initBackup(String pathname, int replicationDegree) {
    File file = new File(pathname);
    if (!file.exists() || !file.isFile()) {
      Peer.log("Invalid backup: could not find file " + pathname, Level.WARNING);
      return;
    }

    try {
      String fileId = Utils.hash(file, Peer.getInstance().getId());
      long length = file.length();
      int numberOfChunks = Utils.numberOfChunks(length);
      FileInfoManager.getInstance().addOwnFileInfo(pathname, fileId,
          numberOfChunks, replicationDegree);
    } catch (Exception e) {
      Peer.log("Failed to hash file " + pathname, e, Level.SEVERE);
      return;
    }

    OwnFileInfo info = FileInfoManager.getInstance().getPathname(pathname);
    if (info == null) {
      Peer.log("Something went wrong setting up own file info...", Level.SEVERE);
      return;
    }

    transmitFile(info, replicationDegree, file);
  }

  private void transmitFile(OwnFileInfo info, int replicationDegree, File fileToBackup) {

    int numberBytesRead;
    int chunkNumber = 0;
    int numberOfChunks = info.getNumberOfChunks();

    System.out.println("Transmitting file...");

    // try-with-resources auto-closes fis.
    try (FileInputStream fis = new FileInputStream(fileToBackup)) {
      do {
        byte[] chunk = new byte[Protocol.chunkSize]; // annoying final warning

        ChunkKey key = new ChunkKey(info.getFileId(), chunkNumber);
        int currentReplicationDegree = info.getChunkReplicationDegree(chunkNumber);

        numberBytesRead = fis.read(chunk, 0, Protocol.chunkSize);

        System.out.println("Read: " + numberBytesRead);

        // Launch the Putchunker only if the perceived replication degree is lower than
        // the desired replication degree, and don't create a second one for the same
        // chunk if one is already running.
        if (currentReplicationDegree < replicationDegree) {
          int len = numberBytesRead < 0 ? 0 : numberBytesRead;
          byte[] trimmed = new byte[len];
          System.arraycopy(chunk, 0, trimmed, 0, len);
          putchunkers.computeIfAbsent(key,
              k -> new Putchunker(key, replicationDegree, trimmed));
        }

        chunkNumber++;
      }
      while (numberBytesRead == Protocol.chunkSize && chunkNumber < numberOfChunks);

      if (chunkNumber != numberOfChunks) {
        Peer.log("Number of chunks read/computed differ for " + fileToBackup,
            Level.SEVERE);
      }
    } catch (IOException e) {
      Peer.log("Could not read from the file '" + fileToBackup, e, Level.SEVERE);
    }
  }
}
