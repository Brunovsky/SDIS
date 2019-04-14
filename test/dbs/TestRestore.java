package dbs;

import dbs.files.FileInfoManager;
import dbs.files.FilesManager;
import dbs.message.Message;
import dbs.transmitter.ChunkTransmitter;
import dbs.transmitter.RestoreHandler;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class TestRestore {
  void init() throws UnknownHostException {
    Protocol.mc = new MulticastChannel(InetAddress.getByName("237.0.0.1"), 29500);
    Protocol.mdb = new MulticastChannel(InetAddress.getByName("237.0.0.2"), 29501);
    Protocol.mdr = new MulticastChannel(InetAddress.getByName("237.0.0.3"), 29502);
    FilesManager.deleteRecursive(Paths.get("/tmp/dbs").toFile());
    config();
  }

  void config() {
    Configuration.allPeersRootDir = "/tmp/dbs";
    Configuration.peerRootDirPrefix = "peer-";
    Configuration.backupDir = "backup";
    Configuration.restoredDir = "restored";
    Configuration.filesinfoDir = "filesinfo";

    Configuration.entryPrefix = "file-";
    Configuration.chunkPrefix = "chunk-";
    Configuration.peerRootDirPrefix = "peer-";
  }

  String hash1 = "1000000000000000000000000000000000000000000000000000000000000000";
  String hash2 = "2000000000000000000000000000000000000000000000000000000000000000";
  String hash3 = "3000000000000000000000000000000000000000000000000000000000000000";

  byte[] body10 = "Body 10 -- text -- text".getBytes();
  byte[] body11 = "Body 11 -- text -- text".getBytes();
  byte[] body20 = "Body 20 -- text -- text".getBytes();
  byte[] body21 = "Body 21 -- text -- text".getBytes();
  byte[] body22 = "Body 22 -- text -- text".getBytes();
  byte[] body30 = "Body 30 -- text -- text -- text".getBytes();

  @Test
  void chunkers() throws Exception {
    init();

    Peer peer = Peer.createInstance(1000, "peer-1000");
    FileInfoManager.createInstance();
    RestoreHandler restorer = RestoreHandler.getInstance();

    Message g10 = Message.GETCHUNK(hash1, "1.0", 0);
    Message g11 = Message.GETCHUNK(hash1, "1.0", 1);
    Message g20 = Message.GETCHUNK(hash2, "1.0", 0);
    Message g21 = Message.GETCHUNK(hash2, "1.0", 1);
    Message g22 = Message.GETCHUNK(hash2, "1.0", 2);
    Message g30 = Message.GETCHUNK(hash3, "1.0", 0);

    Message c10 = Message.CHUNK(hash1, "1.0", 0, body10);
    Message c11 = Message.CHUNK(hash1, "1.0", 1, body11);
    Message c20 = Message.CHUNK(hash2, "1.0", 0, body20);
    Message c21 = Message.CHUNK(hash2, "1.0", 1, body21);
    Message c22 = Message.CHUNK(hash2, "1.0", 2, body22);
    Message c30 = Message.CHUNK(hash3, "1.0", 0, body30);

    ChunkKey k1 = new ChunkKey(hash1, 0);
    ChunkKey k2 = new ChunkKey(hash1, 0);

    assertEquals(k1, k2);
    assertEquals(k1.hashCode(), k2.hashCode());

    int waitChunk = Protocol.maxDelay;

    // The peer doesn't have chunk c10, so the chunker is never created.
    ChunkTransmitter chunker1 = restorer.receiveGETCHUNK(g10);
    assertNull(chunker1);

    // Now the peer has c10.
    FileInfoManager.getInstance().storeChunk(hash1, 0, body10);

    // Now the restorer creates the chunker.
    ChunkTransmitter chunker2 = restorer.receiveGETCHUNK(g10);

    assertNotNull(chunker2);
    assertFalse(chunker2.isDone());

    // The chunk receiver alerts chunker2 that the chunk c10 was detected.
    restorer.receiveCHUNK(c10);

    assertTrue(chunker2.isDone());

    ChunkTransmitter chunker3 = restorer.receiveGETCHUNK(g11);
    FileInfoManager.getInstance().storeChunk(hash1, 1, body11);
    FileInfoManager.getInstance().storeChunk(hash2, 0, body20);
    FileInfoManager.getInstance().storeChunk(hash2, 1, body21);
    FileInfoManager.getInstance().storeChunk(hash2, 2, body22);
    FileInfoManager.getInstance().storeChunk(hash3, 0, body30);
    ChunkTransmitter chunker4 = restorer.receiveGETCHUNK(g20); // exists
    ChunkTransmitter chunker5 = restorer.receiveGETCHUNK(g21); // does not exist
    ChunkTransmitter chunker6 = restorer.receiveGETCHUNK(g22); // exists
    ChunkTransmitter chunker7 = restorer.receiveGETCHUNK(g30); // does not exist, will be
    // deleted
    assertNull(chunker3);
    assertNotNull(chunker4);
    assertNotNull(chunker5);
    assertNotNull(chunker6);
    assertNotNull(chunker7);
    restorer.receiveCHUNK(c20);
    restorer.receiveCHUNK(c22);
    assertTrue(chunker4.isDone());
    assertTrue(chunker6.isDone());
    assertFalse(chunker5.isDone());
    assertFalse(chunker7.isDone());
    FileInfoManager.getInstance().deleteChunk(hash3, 0);

    Thread.sleep(waitChunk * 2, 0);

    assertTrue(chunker5.isDone()); // See the MDR message sent by chunker5
    assertTrue(chunker7.isDone()); // but not by chunker7
  }
}
