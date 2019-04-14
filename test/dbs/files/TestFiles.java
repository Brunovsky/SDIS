package dbs.files;

import dbs.Configuration;
import dbs.MulticastChannel;
import dbs.Peer;
import dbs.Protocol;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

public class TestFiles {
  String hash1 = "0100000000000000000000000000000000000000000000000000000000000000";
  String hash2 = "0200000000000000000000000000000000000000000000000000000000000000";
  String hash3 = "0300000000000000000000000000000000000000000000000000000000000000";
  String hash4 = "0400000000000000000000000000000000000000000000000000000000000000";
  String hash8 = "0800000000000000000000000000000000000000000000000000000000000000";
  String hash9 = "0900000000000000000000000000000000000000000000000000000000000000";

  byte[] b1 = "1111\n".getBytes();
  byte[] b2 = "22222222\n".getBytes();
  byte[] b3 = "333333\n".getBytes();
  byte[] b4 = "4444444444\n".getBytes();
  byte[] b5 = "55\n".getBytes();
  byte[] b6 = "666666666666\n".getBytes();

  byte[][] parts1 = {"foo".getBytes(), "bar".getBytes(), "baz".getBytes()};
  byte[][] parts2 = {"www".getBytes(), "stack".getBytes(), "overflow".getBytes()};
  byte[][] parts3 = {"0".getBytes(), "11".getBytes(), "222".getBytes(),
      "3333".getBytes(), "44444".getBytes(), "555555".getBytes()};
  byte[] p1 = "foobarbaz".getBytes();
  byte[] p2 = "wwwstackoverflow".getBytes();
  byte[] p3 = "011222333344444555555".getBytes();

  String f1 = "hello-world", f2 = "communism", f3 = "socialism", f4 = "dummy";
  String c1 = "hello-world-content", c2 = "communism-content", c3 = "socialism-content";

  void config() throws UnknownHostException {
    Configuration.allPeersRootDir = "/tmp/dbs";
    Configuration.peerRootDirPrefix = "peer-";
    Configuration.backupDir = "backup";
    Configuration.restoredDir = "restored";
    Configuration.filesinfoDir = "filesinfo";

    Configuration.entryPrefix = "file-";
    Configuration.chunkPrefix = "chunk-";
    Configuration.peerRootDirPrefix = "peer-";

    Protocol.mc = new MulticastChannel(InetAddress.getByName("237.0.0.1"), 29500);
    Protocol.mdb = new MulticastChannel(InetAddress.getByName("237.0.0.2"), 29501);
    Protocol.mdr = new MulticastChannel(InetAddress.getByName("237.0.0.3"), 29502);
    FilesManager.deleteRecursive(Paths.get("/tmp/dbs").toFile());
  }

  long PEERID = 1000;
  String ACCESSPOINT = "peer-1000";

  @Test
  void launchAndDelete() throws IOException {
    config();

    Peer.createInstance(PEERID, ACCESSPOINT);
    FilesManager.createInstance();

    String peer1000 = "/tmp/dbs/peer-1000/";
    assertTrue(Files.deleteIfExists(Paths.get(peer1000 + Configuration.backupDir)));
    assertTrue(Files.deleteIfExists(Paths.get(peer1000 + Configuration.restoredDir)));
    assertTrue(Files.deleteIfExists(Paths.get(peer1000 + Configuration.filesinfoDir)));
    assertTrue(Files.deleteIfExists(Paths.get("/tmp/dbs/peer-1000")));

    assertTrue(Files.deleteIfExists(Paths.get("/tmp/dbs")));
  }

  @Test
  void backupOne() throws IOException {
    config();

    Peer.createInstance(PEERID, ACCESSPOINT);
    FilesManager.createInstance();

    assertTrue(FilesManager.getInstance().putChunk(hash1, 1, b1));
    assertTrue(FilesManager.getInstance().putChunk(hash1, 9, b1));
    assertTrue(FilesManager.getInstance().putChunk(hash2, 0, b2));
    assertTrue(FilesManager.getInstance().putChunk(hash2, 1, b4));
    assertTrue(FilesManager.getInstance().putChunk(hash2, 2, b6));
    assertTrue(FilesManager.getInstance().putChunk(hash3, 2, b5));
    assertTrue(FilesManager.getInstance().putChunk(hash4, 1, b1));
    assertTrue(FilesManager.getInstance().putChunk(hash4, 2, b2));
    assertTrue(FilesManager.getInstance().putChunk(hash4, 3, b3));
    assertTrue(FilesManager.getInstance().putChunk(hash4, 4, b4));

    assertArrayEquals(b1, FilesManager.getInstance().getChunk(hash1, 1));
    assertArrayEquals(b5, FilesManager.getInstance().getChunk(hash3, 2));
    assertArrayEquals(b2, FilesManager.getInstance().getChunk(hash4, 2));
    assertNull(FilesManager.getInstance().getChunk(hash1, 2));
    assertNull(FilesManager.getInstance().getChunk(hash4, 5));

    assertTrue(FilesManager.getInstance().hasChunk(hash1, 1));
    assertTrue(FilesManager.getInstance().hasChunk(hash2, 1));
    assertTrue(FilesManager.getInstance().hasChunk(hash4, 4));
    assertFalse(FilesManager.getInstance().hasChunk(hash2, 3));
    assertFalse(FilesManager.getInstance().hasChunk(hash9, 0));

    assertTrue(FilesManager.getInstance().hasBackupFolder(hash1));
    assertTrue(FilesManager.getInstance().hasBackupFolder(hash4));
    assertFalse(FilesManager.getInstance().hasBackupFolder(hash8));

    assertTrue(FilesManager.getInstance().hasChunk(hash1, 9));
    assertTrue(FilesManager.getInstance().deleteChunk(hash1, 9));
    assertFalse(FilesManager.getInstance().hasChunk(hash1, 9));
    assertTrue(FilesManager.getInstance().deleteChunk(hash1, 9));
    assertFalse(FilesManager.getInstance().hasChunk(hash9, 2));
    assertTrue(FilesManager.getInstance().deleteChunk(hash9, 2));
    assertFalse(FilesManager.getInstance().hasChunk(hash9, 2));
    assertFalse(FilesManager.getInstance().hasBackupFolder(hash9));
    assertTrue(FilesManager.getInstance().hasChunk(hash4, 2));
    assertTrue(FilesManager.getInstance().deleteChunk(hash4, 2));

    assertTrue(FilesManager.getInstance().deleteBackupFile(hash1));
    assertFalse(FilesManager.getInstance().hasBackupFolder(hash1));
    assertFalse(FilesManager.getInstance().hasChunk(hash1, 1));
    assertTrue(FilesManager.getInstance().deleteBackupFile(hash2));
    assertFalse(FilesManager.getInstance().hasChunk(hash2, 0));
    assertFalse(FilesManager.getInstance().hasChunk(hash2, 1));
    assertFalse(FilesManager.getInstance().hasChunk(hash2, 2));
    assertFalse(FilesManager.getInstance().hasChunk(hash2, 3));
    assertFalse(FilesManager.getInstance().hasBackupFolder(hash2));
  }

  @Test
  void restore() throws IOException {
    config();

    Peer.createInstance(PEERID, ACCESSPOINT);
    FilesManager.createInstance();

    assertFalse(FilesManager.getInstance().hasRestore("filename-1"));
    assertFalse(FilesManager.getInstance().hasRestore("filename-2"));
    assertFalse(FilesManager.getInstance().hasRestore("filename-9"));

    assertTrue(FilesManager.getInstance().putRestore("filename-1", parts1));
    assertTrue(FilesManager.getInstance().putRestore("filename-2", parts2));
    assertTrue(FilesManager.getInstance().putRestore("filename-3", parts3));

    assertTrue(FilesManager.getInstance().hasRestore("filename-1"));
    assertTrue(FilesManager.getInstance().hasRestore("filename-2"));
    assertFalse(FilesManager.getInstance().hasRestore("filename-9"));

    assertNull(FilesManager.getInstance().getRestore("filename-9"));

    assertArrayEquals(p1, FilesManager.getInstance().getRestore("filename-1"));
    assertArrayEquals(p2, FilesManager.getInstance().getRestore("filename-2"));
    assertArrayEquals(p3, FilesManager.getInstance().getRestore("filename-3"));
  }

  @Test
  void readBulk() throws IOException {
    config();

    Peer.createInstance(PEERID, ACCESSPOINT);
    FilesManager.createInstance();

    TreeSet<String> filesEmpty = new TreeSet<>();
    TreeSet<Integer> chunksEmpty = new TreeSet<>();

    assertEquals(filesEmpty, FilesManager.getInstance().backupFilesSet());
    assertEquals(chunksEmpty, FilesManager.getInstance().backupChunksSet(hash1));
    assertEquals(chunksEmpty, FilesManager.getInstance().backupChunksSet(hash9));

    assertTrue(FilesManager.getInstance().putChunk(hash1, 1, b1));
    assertTrue(FilesManager.getInstance().putChunk(hash1, 9, b2));
    assertTrue(FilesManager.getInstance().putChunk(hash2, 0, b3));
    assertTrue(FilesManager.getInstance().putChunk(hash2, 1, b4));
    assertTrue(FilesManager.getInstance().putChunk(hash2, 2, b5));
    assertTrue(FilesManager.getInstance().putChunk(hash3, 2, b1));
    assertTrue(FilesManager.getInstance().putChunk(hash4, 1, b3));
    assertTrue(FilesManager.getInstance().putChunk(hash4, 2, b4));
    assertTrue(FilesManager.getInstance().putChunk(hash4, 3, b5));
    assertTrue(FilesManager.getInstance().putChunk(hash4, 4, b6));

    TreeSet<String> files = new TreeSet<>(Arrays.asList(hash1, hash2, hash3, hash4));
    TreeSet<Integer> chunks1 = new TreeSet<>(Arrays.asList(1, 9));
    TreeSet<Integer> chunks2 = new TreeSet<>(Arrays.asList(0, 1, 2));
    TreeSet<Integer> chunks3 = new TreeSet<>(Arrays.asList(2));
    TreeSet<Integer> chunks4 = new TreeSet<>(Arrays.asList(1, 2, 3, 4));

    assertEquals(files, FilesManager.getInstance().backupFilesSet());
    assertEquals(chunks1, FilesManager.getInstance().backupChunksSet(hash1));
    assertEquals(chunks2, FilesManager.getInstance().backupChunksSet(hash2));
    assertEquals(chunks3, FilesManager.getInstance().backupChunksSet(hash3));
    assertEquals(chunks4, FilesManager.getInstance().backupChunksSet(hash4));
    assertEquals(chunksEmpty, FilesManager.getInstance().backupChunksSet(hash8));

    HashMap<String, TreeSet<Integer>> map = new HashMap<>(4);
    map.put(hash1, chunks1);
    map.put(hash2, chunks2);
    map.put(hash3, chunks3);
    map.put(hash4, chunks4);

    assertEquals(map, FilesManager.getInstance().backupAllChunksMap());
  }
}
