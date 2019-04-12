package dbs.transmitter;

import dbs.message.Message;

public class BackupHandler {

  private static BackupHandler handler;

  public static BackupHandler getInstance() {
    assert handler != null;
    return handler;
  }

  public static BackupHandler createInstance() {
    return handler == null ? (handler = new BackupHandler()) : handler;
  }

  //final ConcurrentHashMap<ChunkKey,PutchunkTransmitter> putchunkers;

  //final ConcurrentHashMap<ChunkKey,GetchunkTransmitter> storers;

  //final ScheduledThreadPoolExecutor putchunkPool;

  //final ScheduledThreadPoolExecutor storedPool;

  private BackupHandler() {
  }

  // ...
  public void receivePUTCHUNK(Message message) {

  }

  // ...
  public void receiveSTORED(Message message) {

  }

  // ...
  public void initBackup(String pathname, int replicationDegree) {

  }
}
