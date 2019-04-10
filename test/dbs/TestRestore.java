package dbs;

import dbs.files.FilesManager;
import dbs.message.Message;
import dbs.transmitter.DataRestoreTransmitter;
import dbs.transmitter.DataRestoreTransmitter.*;
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
  }

  Configuration config() {
    Configuration config = new Configuration();

    config.allPeersRootDir = "/tmp/dbs";
    config.peerRootDirPrefix = "peer-";
    config.backupDir = "backup";
    config.restoredDir = "restored";
    config.filesinfoDir = "filesinfo";
    config.waitChunk = 1000;

    return config;
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

    Peer peer = new Peer(1, "peer-1", config());
    DataRestoreTransmitter restorer = new DataRestoreTransmitter(peer);

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

    int waitChunk = peer.getConfig().waitChunk;

    // The peer doesn't have chunk c10, so the chunker is never created.
    Chunker chunker1 = restorer.receiveGETCHUNK(g10);
    assertNull(chunker1);

    // Now the peer has c10.
    peer.fileInfoManager.filesManager.putChunk(hash1, 0, body10);

    // Now the restorer creates the chunker.
    Chunker chunker2 = restorer.receiveGETCHUNK(g10);

    assertNotNull(chunker2);
    assertFalse(chunker2.isDone());
    assertEquals(1, peer.getPool().getQueue().size());

    // The chunk receiver alerts chunker2 that the chunk c10 was detected.
    restorer.receiveCHUNK(c10);

    assertTrue(chunker2.isDone());

    Chunker chunker3 = restorer.receiveGETCHUNK(g11);
    peer.fileInfoManager.filesManager.putChunk(hash1, 1, body11);
    peer.fileInfoManager.filesManager.putChunk(hash2, 0, body20);
    peer.fileInfoManager.filesManager.putChunk(hash2, 1, body21);
    peer.fileInfoManager.filesManager.putChunk(hash2, 2, body22);
    peer.fileInfoManager.filesManager.putChunk(hash3, 0, body30);
    Chunker chunker4 = restorer.receiveGETCHUNK(g20); // exists
    Chunker chunker5 = restorer.receiveGETCHUNK(g21); // does not exist
    Chunker chunker6 = restorer.receiveGETCHUNK(g22); // exists
    Chunker chunker7 = restorer.receiveGETCHUNK(g30); // does not exist, will be deleted
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
    peer.fileInfoManager.filesManager.deleteChunk(hash3, 0);

    Thread.sleep(waitChunk * 2, 0);

    assertTrue(chunker5.isDone()); // See the MDR message sent by chunker5
    assertTrue(chunker7.isDone()); // but not by chunker7
  }
}
