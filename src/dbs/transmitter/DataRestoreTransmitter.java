package dbs.transmitter;

import dbs.ChunkKey;
import dbs.Peer;
import dbs.message.Message;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DataRestoreTransmitter {
  private final Peer peer;
  private final ConcurrentHashMap<ChunkKey,AtomicInteger> expectant;

  private final class ChunkResponder implements Runnable {
    private final Message message;
    private final ChunkKey key;

    protected ChunkResponder(@NotNull Message message, @NotNull ChunkKey key) {
      this.message = message;
      this.key = key;
    }

    @Override
    public void run() {
      int expectancy = expectant.getOrDefault()
    }
  }

  public DataRestoreTransmitter(Peer peer) {
    this.peer = peer;
    expectant = new ConcurrentHashMap<>();
  }

  public void getchunkHandler(@NotNull Message message) {
    String fileId = message.getFileId();
    int chunkNo = message.getChunkNo();

    // Exit early if we don't have the chunk.
    if (!peer.getFilesManager().hasChunk(fileId, chunkNo)) return;

    ChunkKey key = new ChunkKey(fileId, chunkNo);
    expectant.putIfAbsent(key, new AtomicInteger(0)).incrementAndGet();
  }
}
