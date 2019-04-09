package dbs.files;

import dbs.Configuration;
import dbs.Peer;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

public final class FilesManager {
  private static final Logger LOGGER = Logger.getLogger(FilesManager.class.getName());
  private final String id;
  private final Configuration config;

  private Path allPeersDir;
  private Path peerDir;
  private Path backupDir;
  private Path restoredDir;
  private Path mineDir;
  private Path idMapDir;

  static private String chk(@NotNull String fileId, int chunkNo) {
    return "chunk #" + chunkNo + " of file " + id(fileId);
  }

  static private String id(@NotNull String fileId) {
    return fileId.substring(0, 10) + "..";
  }

  static byte[] concatenateChunks(byte @NotNull [] @NotNull [] chunks) {
    int length = 0;
    for (byte[] chunk : chunks) length += chunk.length;

    byte[] join = new byte[length];
    length = 0;

    for (byte[] chunk : chunks) {
      System.arraycopy(chunk, 0, join, length, chunk.length);
      length += chunk.length;
    }

    return join;
  }

  static boolean deleteRecursive(@NotNull File file) {
    if (file.isDirectory()) {
      File[] subfiles = file.listFiles();
      if (subfiles != null) {
        for (final File subfile : subfiles) {
          deleteRecursive(subfile);
        }
      }
    }
    return file.delete();
  }

  static boolean deleteDirectory(@NotNull File directory) {
    if (!directory.isDirectory()) return false;
    File[] files = directory.listFiles();
    if (files != null) {
      for (final File file : files) {
        file.delete();
      }
    }
    return directory.delete();
  }

  /**
   * @param fileId A valid fileId
   * @return The corresponding directory name in the backup/ directory
   */
  private String makeBackupEntry(@NotNull String fileId) {
    return config.entryPrefix + fileId;
  }

  /**
   * @param chunkNo A valid chunk number
   * @return The corresponding filename in its file's subdirectory
   */
  private String makeChunkEntry(int chunkNo) {
    return config.chunkPrefix + chunkNo;
  }

  /**
   * Construct a file manager for this given peer.
   * The file manager only needs the peer's configuration and id.
   *
   * @param peer The DBS peer.
   * @throws IOException If the directories cannot be properly set up.
   */
  public FilesManager(@NotNull Peer peer) throws IOException {
    this(Long.toString(peer.getId()), peer.getConfig());
  }

  /**
   * Constructs a files manager for a peer's id and config.
   *
   * @param id     The DBS peer's id.
   * @param config THe configuration (contains paths)
   * @throws IOException If the directories cannot be properly set up.
   */
  FilesManager(@NotNull String id, @NotNull Configuration config) throws IOException {
    this.id = id;
    this.config = config;

    Path path;

    // dbs/
    path = Paths.get(config.allPeersRootDir);
    allPeersDir = Files.createDirectories(path);

    // dbs/peer-ID
    path = allPeersDir.resolve(config.peerRootDirPrefix + this.id);
    peerDir = Files.createDirectories(path);

    // dbs/peer-ID/backup/
    path = peerDir.resolve(config.backupDir);
    backupDir = Files.createDirectories(path);

    // dbs/peer-ID/restored/
    path = peerDir.resolve(config.restoredDir);
    restoredDir = Files.createDirectories(path);

    // dbs/peer-ID/idmap/
    path = peerDir.resolve(config.idMapDir);
    idMapDir = Files.createDirectories(path);

    // dbs/peer-ID/mine/
    path = peerDir.resolve(config.chunkInfoDir);
    mineDir = Files.createDirectories(path);
  }

  /**
   * Verifies if there exists a backup folder corresponding to this file id.
   *
   * @param fileId The file id
   * @return true if the folder exists, and false otherwise.
   */
  public boolean hasBackupFolder(@NotNull String fileId) {
    Path filepath = backupDir.resolve(makeBackupEntry(fileId));
    return Files.exists(filepath) && Files.isDirectory(filepath);
  }

  /**
   * Delete the backup folder and all its chunks.
   *
   * @param fileId The file id
   * @return false if the folder existed and could not be completely deleted, and true
   * otherwise.
   */
  public boolean deleteBackupFile(@NotNull String fileId) {
    Path filepath = backupDir.resolve(makeBackupEntry(fileId));
    return deleteDirectory(filepath.toFile());
  }

  /**
   * Verifies if there exists a chunk file corresponding to this chunk.
   *
   * @param fileId  The file id
   * @param chunkNo The chunk number
   * @return true if the file exists, and false otherwise.
   */
  public boolean hasChunk(@NotNull String fileId, int chunkNo) {
    Path filepath = backupDir.resolve(makeBackupEntry(fileId));
    Path chunkpath = filepath.resolve(makeChunkEntry(chunkNo));
    return Files.exists(chunkpath) && Files.isRegularFile(chunkpath);
  }

  /**
   * Returns the content of this chunk, or null if it does not exist or a reading error
   * occurred.
   *
   * @param fileId  The file id
   * @param chunkNo The chunk number
   * @return The entire chunk content, or null if the chunk does not exist/could not be
   * read.
   */
  public byte[] getChunk(@NotNull String fileId, int chunkNo) {
    try {
      Path filepath = backupDir.resolve(makeBackupEntry(fileId));
      Path chunkpath = filepath.resolve(makeChunkEntry(chunkNo));
      if (Files.notExists(chunkpath)) return null;
      return Files.readAllBytes(chunkpath);
    } catch (IOException e) {
      LOGGER.warning("Failed to get " + chk(fileId, chunkNo) + "\n" + e.getMessage());
      return null;
    }
  }

  /**
   * Stores a new chunk. If another chunk with the same name exists, it will be
   * overwritten.
   *
   * @param fileId  The file id
   * @param chunkNo The chunk number
   * @param chunk   The chunk content
   * @return true if the file was successfully written, false otherwise
   */
  public boolean putChunk(@NotNull String fileId, int chunkNo, byte @NotNull [] chunk) {
    try {
      Path path = backupDir.resolve(makeBackupEntry(fileId));
      Path filepath = Files.createDirectories(path);
      Path chunkpath = filepath.resolve(makeChunkEntry(chunkNo));
      Files.write(chunkpath, chunk);
      return true;
    } catch (IOException e) {
      LOGGER.warning("Failed to put " + chk(fileId, chunkNo) + "\n" + e.getMessage());
      return false;
    }
  }

  /**
   * Delete one chunk.
   *
   * @param fileId  The file id
   * @param chunkNo The chunk number
   * @return false if the file existed and could not be deleted, and true otherwise.
   */
  public boolean deleteChunk(@NotNull String fileId, int chunkNo) {
    try {
      Path filepath = backupDir.resolve(makeBackupEntry(fileId));
      Path chunkpath = filepath.resolve(makeChunkEntry(chunkNo));
      if (Files.notExists(chunkpath)) return true;
      return Files.deleteIfExists(chunkpath);
    } catch (IOException e) {
      LOGGER.warning("Failed to delete " + chk(fileId, chunkNo) + "\n" + e.getMessage());
      return false;
    }
  }

  /**
   * Verifies if there exists a restored file with this filename.
   *
   * @param filename The filename being restored
   * @return true if the file exists, and false otherwise.
   */
  public boolean hasRestore(@NotNull String filename) {
    Path filepath = restoredDir.resolve(filename);
    return Files.exists(filepath);
  }

  /**
   * Returns the content of this restored file, or null if it does not exist or a
   * reading error occurred.
   *
   * @param filename The filename being restored
   * @return The entire file content, or null if it does not exist/could not be read.
   */
  public byte[] getRestore(@NotNull String filename) {
    try {
      Path filepath = restoredDir.resolve(filename);
      if (Files.notExists(filepath)) return null;
      return Files.readAllBytes(filepath);
    } catch (IOException e) {
      LOGGER.warning("Failed to read restore file " + filename + "\n" + e.getMessage());
      return null;
    }
  }

  /**
   * Stores a new restored file. If another file with the same name exists, it will be
   * overwritten.
   *
   * @param filename The file being restored
   * @param chunks   The various chunks composing the file
   * @return true if the file was successfully written, false otherwise
   */
  public boolean putRestore(@NotNull String filename,
                            byte @NotNull [] @NotNull [] chunks) {
    try {
      Path filepath = restoredDir.resolve(filename);
      Files.write(filepath, concatenateChunks(chunks));
      return true;
    } catch (IOException e) {
      LOGGER.warning("Failed to restore file " + filename + "\n" + e.getMessage());
      return false;
    }
  }

  /**
   * Get total amount of disk space occupied by this backup file. Internal auxiliary.
   *
   * @param file A file object, presumably valid and inside the backup/ subdirectory
   * @return The total amount of disk space, 0 if it does not exist or is not a folder.
   */
  private long backupFileTotalSpace(@NotNull File file) {
    if (!file.exists() || !file.isDirectory()) return 0;

    File[] chunks = file.listFiles();
    if (chunks == null) return 0;

    long total = 0;
    for (File chunk : chunks) total += chunk.getTotalSpace();
    return total;
  }

  /**
   * Total amount of space occupied by the file with this id.
   *
   * @param fileId The file id
   * @return The total amount of disk space used, or 0 if it does not exist or is not a
   * folder.
   */
  public long backupFileTotalSpace(@NotNull String fileId) {
    Path filepath = backupDir.resolve(makeBackupEntry(fileId));
    if (Files.notExists(filepath)) return 0;

    File file = filepath.toFile();
    return backupFileTotalSpace(file);
  }

  /**
   * Total amount of space occupied by all files backed up by this peer.
   *
   * @return The total amount of disk space used.
   */
  public long backupTotalSpace() {
    File[] files = backupDir.toFile().listFiles();
    if (files == null) return 0;

    long total = 0;
    for (File file : files) total += backupFileTotalSpace(file);
    return total;
  }

  /**
   * List of files kept by this peer's backup.
   *
   * @return A (possibly empty) list of files kept under backup/
   */
  public File[] backupFilesList() {
    File[] files = backupDir.toFile().listFiles();
    return files == null ? new File[0] : files;
  }

  /**
   * List of chunks under this file.
   *
   * @param fileId The file id
   * @return A (possible empty) list of chunks kept for this file.
   */
  public File[] backupChunksList(@NotNull String fileId) {
    Path filepath = backupDir.resolve(makeBackupEntry(fileId));
    if (Files.notExists(filepath)) return null;

    File[] files = filepath.toFile().listFiles();
    return files == null ? new File[0] : files;
  }

  /**
   * Retrieve the file id corresponding to a given filename.
   *
   * @param filename The backed up filename being queried
   * @return True if the file id of the queried filename exists, or false if it does not exist.
   */
  public boolean hasOwnFilename(@NotNull String filename) {
    Path ownpath = idMapDir.resolve(filename);
    return Files.exists(ownpath);
  }

  public boolean hasOwnFileId(@NotNull String fileId) {
    Path minepath = mineDir.resolve(fileId);
    return Files.exists(minepath);
  }

  public String getOwnFileId(@NotNull String filename) {
    try {
      Path ownpath = idMapDir.resolve(filename);
      if (Files.notExists(ownpath)) return null;
      byte[] bytes = Files.readAllBytes(ownpath);
      return new String(bytes);
    } catch (IOException e) {
      LOGGER.warning("Failed to read idmap file for " + filename + "\n" + e.getMessage());
      return null;
    }
  }

  public boolean putOwnFileId(@NotNull String filename, @NotNull String fileId) {
    try {
      Path ownpath = idMapDir.resolve(filename);
      Files.write(ownpath, fileId.getBytes());
      return true;
    } catch (IOException e) {
      LOGGER.warning("Failed to write idmap file " + filename + "\n" + e.getMessage());
      return false;
    }
  }

  public boolean deleteOwnFileId(@NotNull String filename) {
    try {
      Path ownpath = idMapDir.resolve(filename);
      if (Files.notExists(ownpath)) return true;
      return Files.deleteIfExists(ownpath);
    } catch (IOException e) {
      LOGGER.warning("Failed to delete idmap file " + filename + "\n" + e.getMessage());
      return false;
    }
  }

  public Object getMetadataOfFileId(@NotNull String fileId) {
    Path minepath = mineDir.resolve(fileId);
    if (Files.notExists(minepath)) return null;

    try (
        FileInputStream fis = new FileInputStream(minepath.toFile());
        ObjectInputStream ois = new ObjectInputStream(fis)) {
      return ois.readObject();
    } catch (IOException e) {
      LOGGER.severe("Failed to read metadata for " + id(fileId) + "\n" + e.getMessage());
      return null;
    } catch (ClassNotFoundException e) {
      LOGGER.severe("Invalid class in serializable of " + id(fileId) + "\n" + e.getMessage());
      return null;
    }
  }

  public Object getMetadataOfFilename(@NotNull String filename) {
    String fileId = getOwnFileId(filename);
    if (fileId == null) return null;

    return getMetadataOfFileId(fileId);
  }

  public boolean putMetadataOfFileId(@NotNull String fileId, Serializable ser) {
    Path minepath = mineDir.resolve(fileId);

    try (
        FileOutputStream fos = new FileOutputStream(minepath.toFile());
        ObjectOutputStream oos = new ObjectOutputStream(fos)) {
      oos.writeObject(ser);
      return true;
    } catch (IOException e) {
      LOGGER.severe("Failed to write metadata for " + id(fileId) + "\n" + e.getMessage());
      return false;
    }
  }

  public boolean putMetadataOfFilename(@NotNull String filename, Serializable ser) {
    String fileId = getOwnFileId(filename);
    if (fileId == null) return false;

    return putMetadataOfFileId(fileId, ser);
  }
}
