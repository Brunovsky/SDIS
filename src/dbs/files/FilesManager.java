package dbs.files;

import dbs.Configuration;
import dbs.Peer;
import dbs.Utils;
import dbs.fileInfoManager.ChunkInfo;
import dbs.fileInfoManager.FileInfo;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FilesManager {
  private static final Logger LOGGER = Logger.getLogger(FilesManager.class.getName());

  private static FilesManager manager;

  private final Path backupDir;
  private final Path restoredDir;
  private final Path mineDir;
  private final Path idMapDir;
  private final Path filesinfoDir;
  private final Pattern backupPattern;
  private final Pattern chunkPattern;

  public static FilesManager getInstance() {
    return manager;
  }

  public static FilesManager createInstance() throws IOException {
    return manager == null ? (manager = new FilesManager()) : manager;
  }

  private static String chk(String fileId, int chunkNo) {
    return "chunk #" + chunkNo + " of file " + id(fileId);
  }

  private static String id(String fileId) {
    return fileId.substring(0, 10) + "..";
  }

  private static byte[] concatenateChunks(byte[][] chunks) {
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

  public static boolean deleteRecursive(File file) {
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

  public static boolean deleteDirectory(File directory) {
    if (!directory.isDirectory()) return false;
    File[] files = directory.listFiles();
    if (files != null) {
      for (File file : files) {
        file.delete();
      }
    }
    return directory.delete();
  }

  /**
   * Returns information with respects to the given file path.
   *
   * @param pathname The name of the file's path.
   * @param peerId   The id of the peer which owns the file.
   * @return The information regarding that file if it was successfully retrieved and
   * null otherwise.
   */
  public static FileRequest retrieveFileInfo(String pathname, Long peerId) {
    File file = new File(pathname);

    // check if path name corresponds to a valid file
    if (!file.exists() || file.isDirectory()) {
      Peer.log("Invalid path name", Level.SEVERE);
      return null;
    }

    // hash the pathname
    String fileId;
    try {
      fileId = Utils.hash(file, peerId);
    } catch (Exception e) {
      Peer.log("Could not retrieve a file id for the path name " + pathname,
          Level.SEVERE);
      return null;
    }

    long filesize = file.length();
    Integer numberChunks = Utils.numberOfChunks(filesize);
    return new FileRequest(file, fileId, numberChunks);
  }

  private String makeBackupEntry(String fileId) {
    return Configuration.entryPrefix + fileId;
  }

  private String extractFromBackupEntry(String backupFilename) {
    Matcher matcher = backupPattern.matcher(backupFilename);
    if (!matcher.matches()) return null;

    return matcher.group(1);
  }

  private boolean validBackupEntry(String backupFilename) {
    return backupPattern.matcher(backupFilename).matches();
  }

  private String makeChunkEntry(int chunkNo) {
    return Configuration.chunkPrefix + chunkNo;
  }

  private int extractFromChunkEntry(String chunkFilename) {
    Matcher matcher = chunkPattern.matcher(chunkFilename);
    if (!matcher.matches()) return -1;

    return Integer.parseInt(matcher.group(1));
  }

  private boolean validChunkEntry(String chunkFilename) {
    return chunkPattern.matcher(chunkFilename).matches();
  }

  /**
   * Constructs a files manager for a peer's id and config.
   *
   * @throws IOException If the directories cannot be properly set up.
   */
  private FilesManager() throws IOException {
    Path path;
    String id = Long.toString(Peer.getInstance().getId());

    // dbs/
    path = Paths.get(Configuration.allPeersRootDir);
    Path allPeersDir = Files.createDirectories(path);

    // dbs/peer-ID
    path = allPeersDir.resolve(Configuration.peerRootDirPrefix + id);
    Path peerDir = Files.createDirectories(path);

    // dbs/peer-ID/backup/
    path = peerDir.resolve(Configuration.backupDir);
    backupDir = Files.createDirectories(path);

    // dbs/peer-ID/restored/
    path = peerDir.resolve(Configuration.restoredDir);
    restoredDir = Files.createDirectories(path);

    // dbs/peer-ID/idmap/
    path = peerDir.resolve(Configuration.idMapDir);
    idMapDir = Files.createDirectories(path);

    // dbs/peer-ID/mine/
    path = peerDir.resolve(Configuration.chunkInfoDir);
    mineDir = Files.createDirectories(path);

    // dbs/peer-ID/filesinfo/
    path = peerDir.resolve(Configuration.filesinfoDir);
    filesinfoDir = Files.createDirectories(path);

    // prefix[FILEID]
    backupPattern = Pattern.compile(Pattern.quote(Configuration.entryPrefix) + "([0-9a" +
        "-f]{64})");

    // prefix[CHUNKNO]
    chunkPattern = Pattern.compile(Pattern.quote(Configuration.chunkPrefix) + "([0-9]+)");
  }

  /**
   * Verifies if there exists a backup folder corresponding to this file id.
   *
   * @param fileId The file id
   * @return true if the folder exists, and false otherwise.
   */
  public boolean hasBackupFolder(String fileId) {
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
  public boolean deleteBackupFile(String fileId) {
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
  public boolean hasChunk(String fileId, int chunkNo) {
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
  public byte[] getChunk(String fileId, int chunkNo) {
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
  public boolean putChunk(String fileId, int chunkNo, byte[] chunk) {
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
  public boolean deleteChunk(String fileId, int chunkNo) {
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
  public boolean hasRestore(String filename) {
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
  public byte[] getRestore(String filename) {
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
  public boolean putRestore(String filename,
                            byte[][] chunks) {
    try {
      Path filepath = restoredDir.resolve(filename);
      Files.write(filepath, concatenateChunks(chunks));
      return true;
    } catch (IOException e) {
      LOGGER.warning("Failed to restore file " + filename + "\n" + e.getMessage());
      return false;
    }
  }

  private File[] backupFilterFilesList(File[] files) {
    return Arrays.stream(files)
        .filter(file -> validBackupEntry(file.getName()))
        .toArray(File[]::new);
  }

  private File[] backupFilterChunksList(File[] chunks) {
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

  private File[] backupChunksList(File file) {
    if (!file.exists() || !file.isDirectory()) return new File[0];
    File[] chunks = file.listFiles();
    if (chunks == null) {
      return new File[0];
    } else {
      return backupFilterChunksList(chunks);
    }
  }

  private File[] backupChunksList(String fileId) {
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

  public TreeSet<Integer> backupChunksSet(String fileId) {
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
  private long backupFileTotalSpace(File file) {
    File[] chunks = backupChunksList(file);

    long total = 0;
    for (File chunk : chunks) total += chunk.length();
    return total;
  }

  /**
   * Total amount of space occupied by the backup file with this id.
   *
   * @param fileId The file id
   * @return The total amount of disk space used, or 0 if it does not exist or is not a
   * folder.
   */
  public long backupFileTotalSpace(String fileId) {
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

  public void writeFileDesiredReplicationDegree(String fileId,
                                                Integer desiredReplicationDegree) throws IOException {
    Path fileInfoDir = this.filesinfoDir.resolve(fileId);
    Files.createDirectories(fileInfoDir);
    Path chunkInfoPath = fileInfoDir.resolve(Configuration.desiredReplicationDegreeFile);
    this.writeObject(desiredReplicationDegree, chunkInfoPath.toString());
  }

  public boolean deleteFileInfo(String fileId) {
    Path fileInfoDir = this.filesinfoDir.resolve(fileId);
    File file = fileInfoDir.toFile();
    return FilesManager.deleteRecursive(file);
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

  public ConcurrentHashMap<String,FileInfo> initFilesInfo() throws Exception {
    ConcurrentHashMap<String,FileInfo> fileInfoHashMap = new ConcurrentHashMap<>();
    File filesInfoDir = this.filesinfoDir.toFile();

    for (File fileinfoDir : filesInfoDir.listFiles()) {
      if (fileinfoDir.isDirectory()) {
        String fileId = fileinfoDir.getName();
        Path desiredRDFilename =
            fileinfoDir.toPath().resolve(Configuration.desiredReplicationDegreeFile);
        Integer fileDesiredReplicationDegree =
            (Integer) this.readObject(desiredRDFilename.toFile());
        fileInfoHashMap.put(fileId, new FileInfo(fileDesiredReplicationDegree));
        for (File chunkInfoFile : fileinfoDir.listFiles()) {
          if (chunkInfoFile.getName().equals(Configuration.desiredReplicationDegreeFile))
            continue;
          Integer chunkNumber = Integer.parseInt(chunkInfoFile.getName());
          ChunkInfo chunkInfo = this.readChunkInfo(chunkInfoFile);
          fileInfoHashMap.get(fileId).addFileChunk(chunkNumber, chunkInfo);
        }
      }
    }

    return fileInfoHashMap;
  }
}
