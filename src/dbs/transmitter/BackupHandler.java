package dbs.transmitter;

import dbs.ChunkKey;
import dbs.Configuration;
import dbs.message.Message;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class BackupHandler {

  private static BackupHandler handler;

  public static BackupHandler getInstance() {
    assert handler != null;
    return handler;
  }

  public static BackupHandler createInstance() {
    return handler == null ? (handler = new BackupHandler()) : handler;
  }

  //final ConcurrentHashMap<ChunkKey,@NotNull PutchunkTransmitter> putchunkers;

  //final ConcurrentHashMap<ChunkKey,@NotNull GetchunkTransmitter> storers;

  //final ScheduledThreadPoolExecutor putchunkPool;

  //final ScheduledThreadPoolExecutor storedPool;

  private BackupHandler() {
  }

  // ...
  public void receivePUTCHUNK(@NotNull Message message) {

  }

  // ...
  public void receiveSTORED(@NotNull Message message) {

  }

  // ...
  public void initBackup(@NotNull String pathname, int replicationDegree) {

  }
}
