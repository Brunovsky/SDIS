package dbs.files;

import dbs.Configuration;
import dbs.Peer;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FilesManager {

  private static FilesManager manager;

  private final Path backupDir;
  private final Path restoredDir;
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
    if (file == null) return true;
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

  private static boolean deleteDirectory(File directory) {
    if (directory == null) return true;
    if (!directory.isDirectory()) return false;
    File[] files = directory.listFiles();
    if (files != null) {
      for (File file : files) {
        file.delete();
      }
    }
    return directory.delete();
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

    // dbs/peer-ID/filesinfo/
    path = peerDir.resolve(Configuration.filesinfoDir);
    filesinfoDir = Files.createDirectories(path);

    // prefix[FILEID]
    String backupStr = Pattern.quote(Configuration.entryPrefix) + "([0-9a-fA-F]{64})";
    backupPattern = Pattern.compile(backupStr);

    // prefix[CHUNKNO]
    String chunkStr = Pattern.quote(Configuration.chunkPrefix) + "([0-9]+)";
    chunkPattern = Pattern.compile(chunkStr);

    //backupSpace = (backupTotalSpace());
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
      Peer.log("Failed to get " + chk(fileId, chunkNo), e, Level.WARNING);
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
      Peer.log("Failed to put " + chk(fileId, chunkNo), e, Level.WARNING);
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
      Peer.log("Failed to delete " + chk(fileId, chunkNo), e, Level.WARNING);
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
      Peer.log("Failed to read restore file " + filename, e, Level.WARNING);
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
  public boolean putRestore(String filename, byte[][] chunks) {
    try {
      Path filepath = restoredDir.resolve(filename);
      Files.write(filepath, concatenateChunks(chunks));
      return true;
    } catch (IOException e) {
      Peer.log("Failed to restore file " + filename, e, Level.WARNING);
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

  public long backupChunkTotalSpace(String fileId, int chunkNo) {
    Path filepath = backupDir.resolve(makeBackupEntry(fileId));
    Path chunkpath = filepath.resolve(makeChunkEntry(chunkNo));
    File file = chunkpath.toFile();
    if (!file.exists() || !file.isFile()) return -1;
    return file.length();
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

  public void writeObject(Object object, File objectFile) throws IOException {
    // Note: try-with-resources guarantees if there is an error writing to the streams,
    // they are still correctly closed.
    // https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
    try (FileOutputStream fis = new FileOutputStream(objectFile);
         ObjectOutputStream ois = new ObjectOutputStream(fis)) {
      ois.writeObject(object);
    } catch (IOException e) {
      Peer.log("Failed to write object data from " + objectFile, e, Level.SEVERE);
      throw e;
    }
  }

  public Object readObject(File objectFile) throws IOException {
    // Note: try-with-resources guarantees if there is an error reading from the streams,
    // they are still correctly closed.
    // https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
    try (FileInputStream fis = new FileInputStream(objectFile);
         ObjectInputStream ois = new ObjectInputStream(fis)) {
      return ois.readObject();
    } catch (IOException | ClassNotFoundException e) {
      Peer.log("Failed to read object data from " + objectFile, e, Level.SEVERE);
      throw new IOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  ConcurrentHashMap<String,OwnFileInfo> readOwnFilesInfo() throws IOException {
    Path path = filesinfoDir.resolve(Configuration.ownFilesinfo);
    File file = path.toFile();
    if (!file.exists() || !file.isFile()) return null;

    return (ConcurrentHashMap<String,OwnFileInfo>) this.readObject(file);
  }

  @SuppressWarnings("unchecked")
  ConcurrentHashMap<String,FileInfo> readOtherFilesInfo() throws IOException {
    Path path = filesinfoDir.resolve(Configuration.otherFilesinfo);
    File file = path.toFile();
    if (!file.exists() || !file.isFile()) return null;

    return (ConcurrentHashMap<String,FileInfo>) this.readObject(file);
  }

  void writeOwnFilesInfo(ConcurrentHashMap<String,OwnFileInfo> map) {
    File file = filesinfoDir.resolve(Configuration.ownFilesinfo).toFile();
    try {
      writeObject(map, file);
    } catch (IOException e) {
      Peer.log("Failed to store own files info map at " + file, Level.SEVERE);
    }
  }

  void writeOtherFilesInfo(ConcurrentHashMap<String,FileInfo> map) {
    File file = filesinfoDir.resolve(Configuration.otherFilesinfo).toFile();
    try {
      writeObject(map, file);
    } catch (IOException e) {
      Peer.log("Failed to store others files info map at " + file, Level.SEVERE);
    }
  }
}
