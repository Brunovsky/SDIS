package dbs.files;

import dbs.Configuration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

public class TestFiles {
  String hash1 = "0100000000000000000000000000000000000000000000000000000000000000";
  String hash2 = "0200000000000000000000000000000000000000000000000000000000000000";
  String hash3 = "0300000000000000000000000000000000000000000000000000000000000000";
  String hash4 = "0400000000000000000000000000000000000000000000000000000000000000";
  String hash5 = "0500000000000000000000000000000000000000000000000000000000000000";
  String hash6 = "0600000000000000000000000000000000000000000000000000000000000000";
  String hash7 = "0700000000000000000000000000000000000000000000000000000000000000";
  String hash8 = "0800000000000000000000000000000000000000000000000000000000000000";
  String hash9 = "0900000000000000000000000000000000000000000000000000000000000000";
  String hash10 = "1000000000000000000000000000000000000000000000000000000000000000";
  String hash11 = "1100000000000000000000000000000000000000000000000000000000000000";
  String hash12 = "1200000000000000000000000000000000000000000000000000000000000000";
  String hash13 = "1300000000000000000000000000000000000000000000000000000000000000";
  String hash14 = "1400000000000000000000000000000000000000000000000000000000000000";
  String hash15 = "1500000000000000000000000000000000000000000000000000000000000000";

  byte[] b1 = "1111111111111111\n".getBytes();
  byte[] b2 = "22222222222222222222222222222222222\n".getBytes();
  byte[] b3 = "33333333\n".getBytes();
  byte[] b4 = "444444444444444444444444\n".getBytes();
  byte[] b5 = "55555555555555555555555555555555555555555555555555\n".getBytes();
  byte[] b6 = "6666666666666666666666666666666666666666666\n".getBytes();

  Configuration config() {
    Configuration config = new Configuration();

    config.allPeersRootDir = "/tmp/dbs";
    config.peerRootDirPrefix = "peer-";
    config.backupDir = "backup";
    config.restoredDir = "restored";

    return config;
  }

  void clean() {
    FilesManager.deleteRecursive(Paths.get("/tmp/dbs").toFile());
  }

  @Test
  void launchAndDelete() throws IOException {
    clean();

    Configuration config = config();
    FilesManager manager1 = new FilesManager("1000", config);
    FilesManager manager2 = new FilesManager("2000", config);
    FilesManager manager3 = new FilesManager("3000", config);

    assertTrue(Files.deleteIfExists(Paths.get("/tmp/dbs/peer-1000/backup")));
    assertTrue(Files.deleteIfExists(Paths.get("/tmp/dbs/peer-1000/restored")));
    assertTrue(Files.deleteIfExists(Paths.get("/tmp/dbs/peer-1000")));

    assertTrue(Files.deleteIfExists(Paths.get("/tmp/dbs/peer-2000/backup")));
    assertTrue(Files.deleteIfExists(Paths.get("/tmp/dbs/peer-2000/restored")));
    assertTrue(Files.deleteIfExists(Paths.get("/tmp/dbs/peer-2000")));

    assertTrue(Files.deleteIfExists(Paths.get("/tmp/dbs/peer-3000/backup")));
    assertTrue(Files.deleteIfExists(Paths.get("/tmp/dbs/peer-3000/restored")));
    assertTrue(Files.deleteIfExists(Paths.get("/tmp/dbs/peer-3000")));

    assertTrue(Files.deleteIfExists(Paths.get("/tmp/dbs")));
  }

  void putChunk(FilesManager manager) {
    assertTrue(manager.putChunk(hash1, 1, b1));

    assertTrue(manager.putChunk(hash2, 0, b2));
    assertTrue(manager.putChunk(hash2, 1, b4));
    assertTrue(manager.putChunk(hash2, 2, b6));

    assertTrue(manager.putChunk(hash3, 3, b3));
    assertTrue(manager.putChunk(hash3, 5, b2));

    assertTrue(manager.putChunk(hash4, 1, b1));
    assertTrue(manager.putChunk(hash4, 2, b2));
    assertTrue(manager.putChunk(hash4, 3, b3));
    assertTrue(manager.putChunk(hash4, 4, b4));

    assertTrue(manager.putChunk(hash5, 2, b5));

    assertTrue(manager.putChunk(hash6, 0, b6));

    assertTrue(manager.putChunk(hash7, 3, b2));
    assertTrue(manager.putChunk(hash7, 5, b5));
  }

  void getChunk(FilesManager manager) {
    assertArrayEquals(b1, manager.getChunk(hash1, 1));
    assertArrayEquals(b3, manager.getChunk(hash3, 3));
    assertArrayEquals(b2, manager.getChunk(hash4, 2));
    assertArrayEquals(b5, manager.getChunk(hash5, 2));
    assertArrayEquals(b6, manager.getChunk(hash6, 0));
    assertNull(manager.getChunk(hash1, 2));
    assertNull(manager.getChunk(hash4, 5));
  }

  void hasChunk(FilesManager manager) {
    assertTrue(manager.hasChunk(hash1, 1));
    assertTrue(manager.hasChunk(hash2, 1));
    assertTrue(manager.hasChunk(hash3, 3));
    assertTrue(manager.hasChunk(hash4, 4));
    assertTrue(manager.hasChunk(hash7, 5));
    assertFalse(manager.hasChunk(hash2, 3));
    assertFalse(manager.hasChunk(hash3, 4));
    assertFalse(manager.hasChunk(hash9, 0));
    assertFalse(manager.hasChunk(hash10, 1));
  }

  void hasBackupFolder(FilesManager manager) {
    assertTrue(manager.hasBackupFolder(hash1));
    assertTrue(manager.hasBackupFolder(hash3));
    assertTrue(manager.hasBackupFolder(hash6));
    assertFalse(manager.hasBackupFolder(hash8));
    assertFalse(manager.hasBackupFolder(hash10));
  }

  void deleteChunk(FilesManager manager) {
    assertTrue(manager.hasChunk(hash7, 3));
    assertTrue(manager.deleteChunk(hash7, 3));
    assertFalse(manager.hasChunk(hash7, 3));
    assertTrue(manager.deleteChunk(hash7, 3));

    assertFalse(manager.hasChunk(hash12, 2));
    assertTrue(manager.deleteChunk(hash12, 2));
    assertFalse(manager.hasChunk(hash12, 2));
    assertFalse(manager.hasBackupFolder(hash12));

    assertTrue(manager.hasChunk(hash4, 2));
    assertTrue(manager.deleteChunk(hash4, 2));
  }

  void deleteBackupFile(FilesManager manager) {
    manager.deleteBackupFile(hash1);
    assertFalse(manager.hasBackupFolder(hash1));
    assertFalse(manager.hasChunk(hash1, 1));
    manager.deleteBackupFile(hash2);
    assertFalse(manager.hasChunk(hash2, 0));
    assertFalse(manager.hasChunk(hash2, 1));
    assertFalse(manager.hasChunk(hash2, 2));
    assertFalse(manager.hasChunk(hash2, 3));
    assertFalse(manager.hasBackupFolder(hash2));

  }

  @Test
  void backupTest() throws IOException {
    clean();

    Configuration config = config();
    FilesManager manager = new FilesManager("0", config);

    putChunk(manager);
    getChunk(manager);
    hasChunk(manager);
    hasBackupFolder(manager);
    deleteChunk(manager);
    deleteBackupFile(manager);
  }
}
