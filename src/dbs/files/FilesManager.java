package dbs.files;

import dbs.Configuration;
import dbs.Peer;
import dbs.fileInfoManager.ChunkInfo;
import dbs.fileInfoManager.FileInfo;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FilesManager {
  private static final Logger LOGGER = Logger.getLogger(FilesManager.class.getName());
  private final String id;
  private final Configuration config;

  private final Path allPeersDir;
  private final Path peerDir;
  private final Path backupDir;
  private final Path restoredDir;
  private final Path mineDir;
  private final Path idMapDir;
  private final Path filesinfoDir;
  private final Pattern backupPattern;
  private final Pattern chunkPattern;

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

  public static boolean deleteRecursive(@NotNull File file) {
    if (file.isDirectory()) {
      File[] subfiles = file.listFiles();
      if (subfiles != null) {
        for (File subfile : subfiles) {
          deleteRecursive(subfile);
        }
      }
    }
    return file.delete();
  }

  public static boolean deleteDirectory(@NotNull File directory) {
    if (!directory.isDirectory()) return false;
    File[] files = directory.listFiles();
    if (files != null) {
      for (File file : files) {
        file.delete();
      }
    }
    return directory.delete();
  }

  private String makeBackupEntry(@NotNull String fileId) {
    return config.entryPrefix + fileId;
  }

  private String extractFromBackupEntry(@NotNull String backupFilename) {
    Matcher matcher = backupPattern.matcher(backupFilename);
    if (!matcher.matches()) return null;

    return matcher.group(1);
  }

  private boolean validBackupEntry(@NotNull String backupFilename) {
    return backupPattern.matcher(backupFilename).matches();
  }

  private String makeChunkEntry(int chunkNo) {
    return config.chunkPrefix + chunkNo;
  }

  private int extractFromChunkEntry(@NotNull String chunkFilename) {
    Matcher matcher = chunkPattern.matcher(chunkFilename);
    if (!matcher.matches()) return -1;

    return Integer.parseInt(matcher.group(1));
  }

  private boolean validChunkEntry(@NotNull String chunkFilename) {
    return chunkPattern.matcher(chunkFilename).matches();
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

    // dbs/peer-ID/filesinfo/
    path = peerDir.resolve(config.filesinfoDir);
    filesinfoDir = Files.createDirectories(path);

    // prefix[FILEID]
    backupPattern = Pattern.compile(Pattern.quote(config.entryPrefix) + "([0-9a-f]{64})");

    // prefix[CHUNKNO]
    chunkPattern = Pattern.compile(Pattern.quote(config.chunkPrefix) + "([0-9]+)");
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
      Files.deleteIfExists(chunkpath);
      return true;
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

  private File[] backupFilterFilesList(@NotNull File @NotNull [] files) {
    return Arrays.stream(files)
        .filter(file -> validBackupEntry(file.getName()))
        .toArray(File[]::new);
  }

  private File[] backupFilterChunksList(@NotNull File @NotNull [] chunks) {
    return Arrays.stream(chunks)
        .filter(chunk -> validChunkEntry(chunk.getName()))
        .toArray(File[]::new);
  }

  private File[] backupFilesList() {
    File[] files = backupDir.toFile().listFiles();
    if (files == null) {
      return new File[0];
    } else {
      return backupFilterFilesList(files);
    }
  }

  private File[] backupChunksList(@NotNull File file) {
    if (!file.exists() || !file.isDirectory()) return new File[0];
    File[] chunks = file.listFiles();
    if (chunks == null) {
      return new File[0];
    } else {
      return backupFilterChunksList(chunks);
    }
  }

  private File[] backupChunksList(@NotNull String fileId) {
    Path filepath = backupDir.resolve(makeBackupEntry(fileId));
    if (Files.notExists(filepath)) return new File[0];

    return backupChunksList(filepath.toFile());
  }

  public HashSet<String> backupFilesSet() {
    File[] files = backupFilesList();
    HashSet<String> set = new HashSet<>();

    for (File file : files) {
      set.add(extractFromBackupEntry(file.getName()));
    }

    return set;
  }

  public TreeSet<Integer> backupChunksSet(@NotNull String fileId) {
    File[] files = backupChunksList(fileId);
    TreeSet<Integer> set = new TreeSet<>();

    for (File file : files) {
      set.add(extractFromChunkEntry(file.getName()));
    }

    return set;
  }

  public HashMap<String,TreeSet<Integer>> backupAllChunksMap() {
    HashSet<String> fileSet = backupFilesSet();
    HashMap<String,TreeSet<Integer>> map = new HashMap<>();

    for (String fileId : fileSet) {
      map.put(fileId, backupChunksSet(fileId));
    }

    return map;
  }

  /**
   * Get total amount of disk space occupied by this backup file. Internal auxiliary.
   *
   * @param file A file object, presumably valid and inside the backup/ subdirectory
   * @return The total amount of disk space, 0 if it does not exist or is not a folder.
   */
  private long backupFileTotalSpace(@NotNull File file) {
    File[] chunks = backupChunksList(file);

    long total = 0;
    for (File chunk : chunks) total += chunk.getTotalSpace();
    return total;
  }

  /**
   * Total amount of space occupied by the backup file with this id.
   *
   * @param fileId The file id
   * @return The total amount of disk space used, or 0 if it does not exist or is not a
   * folder.
   */
  public long backupFileTotalSpace(@NotNull String fileId) {
    return backupFileTotalSpace(backupDir.resolve(makeBackupEntry(fileId)).toFile());
  }

  /**
   * Total amount of space occupied by all files in the peer's backup subsystem.
   *
   * @return The total amount of disk space used.
   */
  public long backupTotalSpace() {
    File[] files = backupFilesList();

    long total = 0;
    for (File file : files) total += backupFileTotalSpace(file);
    return total;
  }

  public boolean isKeepingFilename(@NotNull String filename) {
    Path ownpath = idMapDir.resolve(filename);
    return Files.exists(ownpath);
  }

  public boolean isKeepingFileId(@NotNull String fileId) {
    Path minepath = mineDir.resolve(fileId);
    return Files.exists(minepath);
  }

  public String getFileId(@NotNull String filename) {
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

  public boolean putKeepingFile(@NotNull String filename, @NotNull String fileId) {
    try {
      Path ownpath = idMapDir.resolve(filename);
      Files.write(ownpath, fileId.getBytes());
      return true;
    } catch (IOException e) {
      LOGGER.warning("Failed to write idmap file " + filename + "\n" + e.getMessage());
      return false;
    }
  }

  public boolean deleteKeepingFile(@NotNull String filename) {
    if (!isKeepingFilename(filename)) return true;

    String fileId = getFileId(filename);
    try {
      Path ownpath = idMapDir.resolve(filename);
      if (Files.notExists(ownpath)) return true;
      Files.deleteIfExists(ownpath);
      return true;
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
    String fileId = getFileId(filename);
    if (fileId == null) return null;

    return getMetadataOfFileId(fileId);
  }

  public boolean putMetadataOfFileId(@NotNull String fileId, @NotNull Serializable ser) {
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

  public boolean putMetadataOfFilename(@NotNull String filename,
                                       @NotNull Serializable ser) {
    String fileId = getFileId(filename);
    if (fileId == null) return false;

    return putMetadataOfFileId(fileId, ser);
  }

  public boolean deleteMetadataOfFileId(@NotNull String fileId) {
    try {
      Path minepath = mineDir.resolve(fileId);
      if (Files.notExists(minepath)) return true;
      Files.deleteIfExists(minepath);
      return true;
    } catch (IOException e) {
      LOGGER.severe("Failed to delete metadata for " + id(fileId) + "\n" + e.getMessage());
      return false;
    }
  }

  public boolean deleteMetadataOfFilename(@NotNull String filename) {
    String fileId = getFileId(filename);
    if (fileId == null) return true;

    return deleteMetadataOfFileId(filename);
  }

  public void writeObject(Object object, String objectPathName) throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream(objectPathName, false);
    ObjectOutputStream out = new ObjectOutputStream(fileOutputStream);
    out.writeObject(object);
    out.close();
    fileOutputStream.close();
  }

  public void writeChunkInfo(String fileId, Integer chunkNumber, ChunkInfo chunkInfo) throws IOException {
    Path fileInfoDir = this.filesinfoDir.resolve(fileId);
    Path chunkInfoPath = fileInfoDir.resolve(chunkNumber.toString());
    Files.createDirectories(fileInfoDir);
    this.writeObject(chunkInfo, chunkInfoPath.toString());
  }

  public void writeFileDesiredReplicationDegree(String fileId, Integer desiredReplicationDegree) throws IOException {
    Path fileInfoDir = this.filesinfoDir.resolve(fileId);
    Files.createDirectories(fileInfoDir);
    Path chunkInfoPath = fileInfoDir.resolve(config.desiredReplicationDegreeFile);
    this.writeObject(desiredReplicationDegree, chunkInfoPath.toString());
  }

  public Object readObject(File objectFile) throws Exception {
    FileInputStream fis = new FileInputStream(objectFile);
    ObjectInputStream ois = new ObjectInputStream(fis);
    Object object = ois.readObject();
    fis.close();
    ois.close();
    return object;
  }

  public ChunkInfo readChunkInfo(File chunkInfoFile) throws Exception {
    return (ChunkInfo) this.readObject(chunkInfoFile);
  }

  public Integer readDesiredReplicationDegree(File desiredReplicationDegreeFile) throws Exception {
    return (Integer) this.readObject(desiredReplicationDegreeFile);
  }

  public ConcurrentHashMap<String, FileInfo> initFilesInfo() throws Exception {
    ConcurrentHashMap<String, FileInfo> fileInfoHashMap = new ConcurrentHashMap<>();
    File filesInfoDir = this.filesinfoDir.toFile();

    for(File fileinfoDir : filesInfoDir.listFiles()) {
      if(fileinfoDir.isDirectory()) {
        String fileId = fileinfoDir.getName();
        Path desiredRDFilename = fileinfoDir.toPath().resolve(config.desiredReplicationDegreeFile);
        Integer fileDesiredReplicationDegree = (Integer) this.readObject(desiredRDFilename.toFile());
        fileInfoHashMap.put(fileId, new FileInfo(fileDesiredReplicationDegree));
        for(File chunkInfoFile : fileinfoDir.listFiles()) {
          if(chunkInfoFile.getName().equals(config.desiredReplicationDegreeFile)) continue;
          Integer chunkNumber = Integer.parseInt(chunkInfoFile.getName());
          ChunkInfo chunkInfo = this.readChunkInfo(chunkInfoFile);
          fileInfoHashMap.get(fileId).addFileChunk(chunkNumber, chunkInfo);
        }
      }
    }

    return fileInfoHashMap;
  }
}
