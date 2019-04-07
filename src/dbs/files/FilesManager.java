package dbs.files;

import dbs.Configuration;
import dbs.Peer;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
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

  private static String chk(@NotNull String fileId, int chunkNo) {
    return "chunk #" + chunkNo + " of file " + fileId.substring(0, 12) + "..";
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
   * Construct a file manager for this given peer.
   * The file manager only needs the peer's configuration and id.
   * @param peer The DBS peer.
   * @throws IOException If the directories cannot be properly set up.
   */
  public FilesManager(@NotNull Peer peer) throws IOException {
    this(Long.toString(peer.getId()), peer.getConfig());
  }

  /**
   * @param id     The DBS peer's id.
   * @param config THe configuration (contains paths)
   * @throws IOException If the directories cannot be properly set up.
   */
  FilesManager(@NotNull String id, @NotNull Configuration config) throws IOException {
    this.id = id;
    this.config = config;

    createAllPeersRoot();
    createPeerRoot();
    createBackupRoot();
    createRestoredRoot();
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
   * Create the root directory shared by all peers if it does not exist.
   * @throws IOException If there is a problem setting up this directory.
   */
  private void createAllPeersRoot() throws IOException {
    Path path = Paths.get(config.allPeersRootDir);
    allPeersDir = Files.createDirectories(path);
  }

  /**
   * Create the root directory for our peer.
   * @throws IOException If there is a problem setting up this directory.
   */
  private void createPeerRoot() throws IOException {
    String subfolder = config.peerRootDirPrefix + id;
    Path path = allPeersDir.resolve(subfolder);
    peerDir = Files.createDirectories(path);
  }

  /**
   * Create the root directory for the backup subprotocol
   * @throws IOException If there is a problem setting up this directory.
   */
  private void createBackupRoot() throws IOException {
    String subfolder = config.backupDir;
    Path path = peerDir.resolve(subfolder);
    backupDir = Files.createDirectories(path);
  }

  /**
   * Create the root directory for the restore subprotocol
   * @throws IOException If there is a problem setting up this directory.
   */
  private void createRestoredRoot() throws IOException {
    String subfolder = config.restoredDir;
    Path path = peerDir.resolve(subfolder);
    restoredDir = Files.createDirectories(path);
  }

  /**
   * Verifies if there exists a backup folder corresponding to this file id.
   * @param fileId The file id
   * @return true if the folder exists, and false otherwise.
   */
  public boolean hasBackupFolder(@NotNull String fileId) {
    Path filepath = backupDir.resolve(makeBackupEntry(fileId));
    return Files.exists(filepath) && Files.isDirectory(filepath);
  }

  /**
   * Verifies if there exists a chunk file corresponding to this chunk.
   * @param fileId The file id
   * @param chunkNo The chunk number
   * @return true if the file exists, and false otherwise.
   */
  public boolean hasChunk(@NotNull String fileId, int chunkNo) {
    Path filepath = backupDir.resolve(makeBackupEntry(fileId));
    Path chunkpath = filepath.resolve(makeChunkEntry(chunkNo));
    return Files.exists(chunkpath) && Files.isRegularFile(chunkpath);
  }

  /**
   * Verifies if there exists a restored file with this filename.
   * @param filename The filename being restored
   * @return true if the file exists, and false otherwise.
   */
  public boolean hasRestore(@NotNull String filename) {
    Path filepath = restoredDir.resolve(filename);
    return Files.exists(filepath);
  }

  /**
   * Returns the content of this chunk, or null if it does not exist or a reading error
   * occurred.
   * @param fileId The file id
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
   * Returns the content of this restored file, or null if it does not exist or a
   * reading error occurred.
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
   * Stores a new chunk. If another chunk with the same name exists, it will be
   * overwritten.
   * @param fileId The file id
   * @param chunkNo The chunk number
   * @param chunk The chunk content
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
   * Stores a new restored file. If another file with the same name exists, it will be
   * overwritten.
   * @param filename The file being restored
   * @param chunks The various chunks composing the file
   * @return true if the file was successfully written, false otherwise
   */
  public boolean putRestore(@NotNull String filename, byte @NotNull [] @NotNull [] chunks) {
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
   * Delete the backup folder and all its chunks.
   * @param fileId The file id
   * @return false if the folder existed and could not be completely deleted, and true
   * otherwise.
   */
  public boolean deleteBackupFile(@NotNull String fileId) {
    Path filepath = backupDir.resolve(makeBackupEntry(fileId));
    return deleteDirectory(filepath.toFile());
  }

  /**
   * Delete one chunk.
   * @param fileId The file id
   * @param chunkNo The chunk number
   * @return false if the file existed and could not be deleted, and true otherwise.
   */
  public boolean deleteChunk(@NotNull String fileId, int chunkNo) {
    try {
      Path filepath = backupDir.resolve(makeBackupEntry(fileId));
      Path chunkpath = filepath.resolve(makeChunkEntry(chunkNo));
      if (!Files.exists(chunkpath)) return true;
      return Files.deleteIfExists(chunkpath);
    } catch (IOException e) {
      LOGGER.warning("Failed to delete " + chk(fileId, chunkNo) + "\n" + e.getMessage());
      return false;
    }
  }

  /**
   * Get total amount of disk space occupied by this backup file. Internal auxiliary.
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
   * @return A (possibly empty) list of files kept under backup/
   */
  public File[] backupFilesList() {
    File[] files = backupDir.toFile().listFiles();
    return files == null ? new File[0] : files;
  }

  /**
   * List of chunks under this file.
   * @param fileId The file id
   * @return A (possible empty) list of chunks kept for this file.
   */
  public File[] backupChunksList(@NotNull String fileId) {
    Path filepath = backupDir.resolve(makeBackupEntry(fileId));
    if (Files.notExists(filepath)) return null;

    File[] files = filepath.toFile().listFiles();
    return files == null ? new File[0] : files;
  }
}
