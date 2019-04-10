package dbs.files;

import dbs.Configuration;
import dbs.fileInfoManager.ChunkInfo;
import dbs.fileInfoManager.FileInfo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

  Configuration config() {
    Configuration config = new Configuration();

    config.allPeersRootDir = "/tmp/dbs";
    config.peerRootDirPrefix = "peer-";
    config.backupDir = "backup";
    config.restoredDir = "restored";
    config.filesinfoDir = "filesinfo";

    return config;
  }

  void clean() {
    FilesManager.deleteRecursive(Paths.get("/tmp/dbs").toFile());
  }

  @Test
  void launchAndDelete() throws IOException {
    clean();

    Configuration config = config();
    new FilesManager("1000", config);
    new FilesManager("2000", config);

    String peer1000 = "/tmp/dbs/peer-1000/";
    assertTrue(Files.deleteIfExists(Paths.get(peer1000 + config.backupDir)));
    assertTrue(Files.deleteIfExists(Paths.get(peer1000 + config.restoredDir)));
    assertTrue(Files.deleteIfExists(Paths.get(peer1000 + config.chunkInfoDir)));
    assertTrue(Files.deleteIfExists(Paths.get(peer1000 + config.idMapDir)));
    assertTrue(Files.deleteIfExists(Paths.get("/tmp/dbs/peer-1000")));

    String peer2000 = "/tmp/dbs/peer-2000/";
    assertTrue(Files.deleteIfExists(Paths.get(peer2000 + config.backupDir)));
    assertTrue(Files.deleteIfExists(Paths.get(peer2000 + config.restoredDir)));
    assertTrue(Files.deleteIfExists(Paths.get(peer2000 + config.chunkInfoDir)));
    assertTrue(Files.deleteIfExists(Paths.get(peer2000 + config.idMapDir)));
    assertTrue(Files.deleteIfExists(Paths.get("/tmp/dbs/peer-2000")));

    assertTrue(Files.deleteIfExists(Paths.get("/tmp/dbs")));
  }

  @Test
  void backupOne() throws IOException {
    clean();

    Configuration config = config();
    FilesManager manager = new FilesManager("1", config);

    assertTrue(manager.putChunk(hash1, 1, b1));
    assertTrue(manager.putChunk(hash1, 9, b1));
    assertTrue(manager.putChunk(hash2, 0, b2));
    assertTrue(manager.putChunk(hash2, 1, b4));
    assertTrue(manager.putChunk(hash2, 2, b6));
    assertTrue(manager.putChunk(hash3, 2, b5));
    assertTrue(manager.putChunk(hash4, 1, b1));
    assertTrue(manager.putChunk(hash4, 2, b2));
    assertTrue(manager.putChunk(hash4, 3, b3));
    assertTrue(manager.putChunk(hash4, 4, b4));

    assertArrayEquals(b1, manager.getChunk(hash1, 1));
    assertArrayEquals(b5, manager.getChunk(hash3, 2));
    assertArrayEquals(b2, manager.getChunk(hash4, 2));
    assertNull(manager.getChunk(hash1, 2));
    assertNull(manager.getChunk(hash4, 5));

    assertTrue(manager.hasChunk(hash1, 1));
    assertTrue(manager.hasChunk(hash2, 1));
    assertTrue(manager.hasChunk(hash4, 4));
    assertFalse(manager.hasChunk(hash2, 3));
    assertFalse(manager.hasChunk(hash9, 0));

    assertTrue(manager.hasBackupFolder(hash1));
    assertTrue(manager.hasBackupFolder(hash4));
    assertFalse(manager.hasBackupFolder(hash8));

    assertTrue(manager.hasChunk(hash1, 9));
    assertTrue(manager.deleteChunk(hash1, 9));
    assertFalse(manager.hasChunk(hash1, 9));
    assertTrue(manager.deleteChunk(hash1, 9));
    assertFalse(manager.hasChunk(hash9, 2));
    assertTrue(manager.deleteChunk(hash9, 2));
    assertFalse(manager.hasChunk(hash9, 2));
    assertFalse(manager.hasBackupFolder(hash9));
    assertTrue(manager.hasChunk(hash4, 2));
    assertTrue(manager.deleteChunk(hash4, 2));

    assertTrue(manager.deleteBackupFile(hash1));
    assertFalse(manager.hasBackupFolder(hash1));
    assertFalse(manager.hasChunk(hash1, 1));
    assertTrue(manager.deleteBackupFile(hash2));
    assertFalse(manager.hasChunk(hash2, 0));
    assertFalse(manager.hasChunk(hash2, 1));
    assertFalse(manager.hasChunk(hash2, 2));
    assertFalse(manager.hasChunk(hash2, 3));
    assertFalse(manager.hasBackupFolder(hash2));
  }

  @Test
  void restore() throws IOException {
    clean();

    Configuration config = config();
    FilesManager manager = new FilesManager("2", config);

    assertFalse(manager.hasRestore("filename-1"));
    assertFalse(manager.hasRestore("filename-2"));
    assertFalse(manager.hasRestore("filename-9"));

    assertTrue(manager.putRestore("filename-1", parts1));
    assertTrue(manager.putRestore("filename-2", parts2));
    assertTrue(manager.putRestore("filename-3", parts3));

    assertTrue(manager.hasRestore("filename-1"));
    assertTrue(manager.hasRestore("filename-2"));
    assertFalse(manager.hasRestore("filename-9"));

    assertNull(manager.getRestore("filename-9"));

    assertArrayEquals(p1, manager.getRestore("filename-1"));
    assertArrayEquals(p2, manager.getRestore("filename-2"));
    assertArrayEquals(p3, manager.getRestore("filename-3"));
  }

  @Test
  void meta() throws IOException {
    clean();

    Configuration config = config();
    FilesManager manager = new FilesManager("3", config);

    assertFalse(manager.isKeepingFilename(f1));
    assertFalse(manager.isKeepingFilename(f2));

    assertTrue(manager.putKeepingFile(f1, hash1));
    assertTrue(manager.putKeepingFile(f2, hash2));
    assertTrue(manager.putKeepingFile(f3, hash3));

    assertEquals(hash1, manager.getFileId(f1));
    assertEquals(hash2, manager.getFileId(f2));
    assertEquals(hash3, manager.getFileId(f3));

    assertNull(manager.getFileId(f4));

    assertTrue(manager.putMetadataOfFileId(hash1, c1));
    assertTrue(manager.putMetadataOfFilename(f2, c2));
    assertTrue(manager.putMetadataOfFilename(f3, c3));

    assertTrue(manager.isKeepingFilename(f1));
    assertTrue(manager.isKeepingFilename(f3));
    assertFalse(manager.isKeepingFilename(f4));

    assertEquals(c1, manager.getMetadataOfFilename(f1));
    assertEquals(c2, manager.getMetadataOfFilename(f2));
    assertEquals(c3, manager.getMetadataOfFileId(hash3));
  }

  @Test
  void readBulk() throws IOException {
    clean();

    Configuration config = config();
    FilesManager manager = new FilesManager("4", config);

    TreeSet<String> filesEmpty = new TreeSet<>();
    TreeSet<Integer> chunksEmpty = new TreeSet<>();

    assertEquals(filesEmpty, manager.backupFilesSet());
    assertEquals(chunksEmpty, manager.backupChunksSet(hash1));
    assertEquals(chunksEmpty, manager.backupChunksSet(hash9));

    assertTrue(manager.putChunk(hash1, 1, b1));
    assertTrue(manager.putChunk(hash1, 9, b2));
    assertTrue(manager.putChunk(hash2, 0, b3));
    assertTrue(manager.putChunk(hash2, 1, b4));
    assertTrue(manager.putChunk(hash2, 2, b5));
    assertTrue(manager.putChunk(hash3, 2, b1));
    assertTrue(manager.putChunk(hash4, 1, b3));
    assertTrue(manager.putChunk(hash4, 2, b4));
    assertTrue(manager.putChunk(hash4, 3, b5));
    assertTrue(manager.putChunk(hash4, 4, b6));

    TreeSet<String> files = new TreeSet<>(Arrays.asList(hash1, hash2, hash3, hash4));
    TreeSet<Integer> chunks1 = new TreeSet<>(Arrays.asList(1, 9));
    TreeSet<Integer> chunks2 = new TreeSet<>(Arrays.asList(0, 1, 2));
    TreeSet<Integer> chunks3 = new TreeSet<>(Arrays.asList(2));
    TreeSet<Integer> chunks4 = new TreeSet<>(Arrays.asList(1, 2, 3, 4));

    assertEquals(files, manager.backupFilesSet());
    assertEquals(chunks1, manager.backupChunksSet(hash1));
    assertEquals(chunks2, manager.backupChunksSet(hash2));
    assertEquals(chunks3, manager.backupChunksSet(hash3));
    assertEquals(chunks4, manager.backupChunksSet(hash4));
    assertEquals(chunksEmpty, manager.backupChunksSet(hash8));

    HashMap<String, TreeSet<Integer>> map = new HashMap<>(4);
    map.put(hash1, chunks1);
    map.put(hash2, chunks2);
    map.put(hash3, chunks3);
    map.put(hash4, chunks4);

    assertEquals(map, manager.backupAllChunksMap());
  }

  @Test
  void writeChunkInfo() throws Exception {
    clean();

    int chunkNumber = 1;
    String peerId = "4";
    String chunkInfoDir = "/tmp/dbs/peer-" + peerId + "/" + config().filesinfoDir + "/" + hash1;
    String backupPeer1 = "1";
    String backupPeer2 = "4";
    Configuration config = config();
    FilesManager manager = new FilesManager(peerId, config);
    Integer desiredReplicationDegree = 2;

    ChunkInfo chunkInfo = new ChunkInfo();
    chunkInfo.addBackupPeer(Long.parseLong(backupPeer1));
    chunkInfo.addBackupPeer(Long.parseLong(backupPeer2));

    manager.writeChunkInfo(hash1, chunkNumber, chunkInfo);
    manager.writeFileDesiredReplicationDegree(hash1, desiredReplicationDegree);
    Path chunkInfoPath = Paths.get(chunkInfoDir).resolve(Integer.toString(chunkNumber));
    Path desiredReplicationDegreePath = Paths.get(chunkInfoDir).resolve(config.desiredReplicationDegreeFile);
    assertTrue(Files.exists(chunkInfoPath));
    assertTrue(Files.exists(desiredReplicationDegreePath));
    manager.readChunkInfo(chunkInfoPath.toFile());
  }

  @Test
  void initFilesInfo() throws Exception {
    clean();
    writeChunkInfo();
    int chunkNumber = 1;
    String peerId = "4";
    Configuration config = config();
    FilesManager manager = new FilesManager(peerId, config);
    HashMap<String, FileInfo> map = manager.initFilesInfo();
    String backupPeer1 = "1";
    String backupPeer2 = "4";

    assertTrue(map.containsKey(hash1));
    assertTrue(map.get(hash1).hasBackupPeer(chunkNumber,Long.parseLong(backupPeer1)));
    assertTrue(map.get(hash1).hasBackupPeer(chunkNumber,Long.parseLong(backupPeer2)));
    assertEquals(2, map.get(hash1).getChunkReplicationDegree(chunkNumber));
    assertEquals(2, map.get(hash1).getDesiredReplicationDegree());
  }
}
