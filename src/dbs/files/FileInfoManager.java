package dbs.files;

import dbs.Peer;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class FileInfoManager {

  private static FileInfoManager manager;

  /**
   * Maps the id of a file to the information of that file, for files this peer has
   * initiated a backup.
   * This is useful in order to keep track of the actual number of peers which back up
   * our files, and to compare it against the desired replication degree of the chunks.
   * The information regarding the files owned by us is also tracked.
   */
  private ConcurrentHashMap<String,OwnFileInfo> ownFilesInfo;

  /**
   * Map for files owned by other peers, whose chunks this peer is backing up.
   */
  private ConcurrentHashMap<String,FileInfo> otherFilesInfo;

  /**
   * Map of path names to file FileInfo instances.
   */
  private final ConcurrentHashMap<String,OwnFileInfo> pathnameMap;

  public static FileInfoManager getInstance() {
    return manager;
  }

  public static FileInfoManager createInstance() throws Exception {
    return manager == null ? (manager = new FileInfoManager()) : manager;
  }

  /**
   * Constructs a new object of the FileInfoManager class.
   */
  private FileInfoManager() throws Exception {
    this.ownFilesInfo = FilesManager.createInstance().readOwnFilesInfo();
    this.otherFilesInfo = FilesManager.getInstance().readOtherFilesInfo();

    if (this.ownFilesInfo == null) this.ownFilesInfo = new ConcurrentHashMap<>();
    if (this.otherFilesInfo == null) this.otherFilesInfo = new ConcurrentHashMap<>();

    // Populate pathname Map.
    this.pathnameMap = new ConcurrentHashMap<>();
    for (Map.Entry<String,OwnFileInfo> entry : ownFilesInfo.entrySet()) {
      OwnFileInfo info = entry.getValue();
      pathnameMap.put(info.getPathname(), info);
    }

    Runtime.getRuntime().addShutdownHook(new Thread(this::storeState));
  }

  // ***** Methods concerning map entries (has, get, add, delete on the maps)

  /**
   * Returns true if the path name map contains an entry for this path name.
   *
   * @param pathname The path name being queried
   * @return true if there is an entry, false otherwise.
   */
  public boolean hasPathname(String pathname) {
    synchronized (pathnameMap) {
      return pathnameMap.containsKey(pathname);
    }
  }

  /**
   * Returns true if the own map contains an entry for the given file id.
   *
   * @param fileId The file's id.
   * @return True if the map contains an entry for the given chunk and false otherwise.
   */
  public boolean hasOwnFileInfo(String fileId) {
    return this.ownFilesInfo.containsKey(fileId);
  }

  /**
   * Returns true if the others map contains an entry for the given file id.
   *
   * @param fileId The file's id.
   * @return True if the map contains an entry for the given chunk and false otherwise.
   */
  public boolean hasOtherFileInfo(String fileId) {
    return this.otherFilesInfo.containsKey(fileId);
  }

  public OwnFileInfo getPathname(String pathname) {
    return pathnameMap.get(pathname);
  }

  /**
   * Returns true if the own map contains an entry for the given file id.
   *
   * @param fileId The file's id.
   * @return True if the map contains an entry for the given chunk and false otherwise.
   */
  public OwnFileInfo getOwnFileInfo(String fileId) {
    return this.ownFilesInfo.get(fileId);
  }

  /**
   * Returns true if the others map contains an entry for the given file id.
   *
   * @param fileId The file's id.
   * @return True if the map contains an entry for the given chunk and false otherwise.
   */
  public FileInfo getOtherFileInfo(String fileId) {
    return this.otherFilesInfo.get(fileId);
  }

  public FileInfo getFileInfo(String fileId) {
    FileInfo info = getOwnFileInfo(fileId);
    if (info != null) return info;
    return getOtherFileInfo(fileId);
  }

  /**
   * Adds a new entry to the ownFilesInfo map if it doesn't exist yet.
   *
   * @param pathname       The original path name.
   * @param fileId         The id of the new file.
   * @param numberOfChunks The file's chunk count
   */
  public void addOwnFileInfo(String pathname, String fileId,
                             int numberOfChunks, int desired) {
    synchronized (pathnameMap) {
      OwnFileInfo info = this.ownFilesInfo.computeIfAbsent(fileId,
          f -> new OwnFileInfo(pathname, fileId, numberOfChunks, desired));
      pathnameMap.put(fileId, info);
    }
  }

  /**
   * Adds a new entry to the otherFilesInfo map if it doesn't exist yet.
   *
   * @param fileId The id of the new file.
   */
  public void addOtherFileInfo(String fileId, int desired) {
    this.otherFilesInfo.computeIfAbsent(fileId, k -> new FileInfo(fileId, desired));
  }

  public void deleteOwnFile(String fileId) {
    synchronized (pathnameMap) {
      OwnFileInfo info = this.ownFilesInfo.remove(fileId);
      if (info != null) pathnameMap.remove(info.getPathname());
    }
  }

  public void deleteOtherFile(String fileId) {
    otherFilesInfo.remove(fileId);
    FilesManager.getInstance().deleteBackupFile(fileId);
  }

  // ***** Methods concerning file chunks we have stored (others)

  /**
   * Returns true if the given chunk is backed up by this peer and false otherwise.
   *
   * @param fileId      The file's id.
   * @param chunkNumber The chunk's number.
   * @return True if the given chunk is backed up by this peer and false otherwise.
   */
  public boolean hasChunk(String fileId, Integer chunkNumber) {
    return FilesManager.getInstance().hasChunk(fileId, chunkNumber);
  }

  /**
   * Stores a new chunk. If another chunk with the same name exists, it will be
   * overwritten.
   * TODO: fix race
   *
   * @param fileId      The file's id
   * @param chunkNumber The chunk's number
   * @param chunk       The chunk's content
   */
  public void storeChunk(String fileId, Integer chunkNumber, byte[] chunk) {
    FileInfo info = this.otherFilesInfo.computeIfAbsent(fileId, FileInfo::new);
    info.addChunkInfo(chunkNumber);
    info.addBackupPeer(chunkNumber, Peer.getInstance().getId());
    FilesManager.getInstance().putChunk(fileId, chunkNumber, chunk);
  }

  /**
   * Deletes a stored chunk.
   *
   * @param fileId      The file's id
   * @param chunkNumber The chunk's number
   * @return true if successful, and false otherwise
   */
  public boolean deleteChunk(String fileId, Integer chunkNumber) {
    FileInfo info = this.otherFilesInfo.get(fileId);
    if (info == null) return true;
    info.removeBackupPeer(chunkNumber, Peer.getInstance().getId());
    return FilesManager.getInstance().deleteChunk(fileId, chunkNumber);
  }

  /**
   * Deletes the provided file.
   *
   * @param file The file to delete.
   * @return true if successfully deleted, false otherwise
   */
  public boolean deleteFile(File file) {
    if (FilesManager.deleteRecursive(file))
      Peer.log("Successfully deleted file " + file.getPath() + " and its records",
          Level.INFO);
    else {
      Peer.log("Could not delete file " + file.getPath() + " and its records",
          Level.SEVERE);
      return false;
    }
    return true;
  }

  // ***** Methods concerning replication degrees

  /**
   * Updates the desired replication degree of a file
   *
   * @param fileId  The file's id.
   * @param desired The desired replication degree of that file.
   */
  public void setDesiredReplicationDegree(String fileId, Integer desired) {
    FileInfo info = this.otherFilesInfo.computeIfAbsent(fileId, FileInfo::new);
    info.setDesiredReplicationDegree(desired);
  }

  /**
   * Returns the number of peer's which have a backup of the given chunk (the actual
   * replication degree of the chunk).
   *
   * @param fileId      The id of the file to which that chunk belongs.
   * @param chunkNumber The chunk's number.
   * @return The number of peer's which have a backup of that chunk (the actual
   * replication degree of the chunk).
   */
  public int getChunkReplicationDegree(String fileId, Integer chunkNumber) {
    // We do not know if this fileId is ours or someone else's. We must check that.
    FileInfo own = this.ownFilesInfo.get(fileId);

    // It is our file.
    if (own != null) {
      return own.getChunkReplicationDegree(chunkNumber);
    } else {
      // It is someone's else, possibly unknown.
      FileInfo other = this.otherFilesInfo.get(fileId);
      return other == null ? 0 : other.getChunkReplicationDegree(chunkNumber);
    }
  }

  /**
   * Returns the desired replication degree of the given file or null if that file is
   * unknown to the peer.
   *
   * @param fileId The file's id.
   * @return The desired replication degree of the given file or null if that file is
   * unknown to the peer.
   */
  public int getDesiredReplicationDegree(String fileId) {
    // We do not know if this fileId is ours or someone else's. We must check that.
    FileInfo own = this.ownFilesInfo.get(fileId);

    // It is our file.
    if (own != null) {
      return own.getDesiredReplicationDegree();
    } else {
      // It is someone's else, possibly unknown.
      FileInfo other = this.otherFilesInfo.get(fileId);
      return other == null ? 0 : other.getDesiredReplicationDegree();
    }
  }

  // ***** Methods concerning backup peers

  /**
   * Returns true if the given peer has a backup of the given chunk (identified by its
   * number and the id of the file to which it belongs).
   *
   * @param fileId      The file's id.
   * @param chunkNumber The chunk's number.
   * @param peerId      The peer's id.
   * @return True if the given peer has a backup of the given chunk.
   */
  public boolean hasBackupPeer(String fileId, Integer chunkNumber, Long peerId) {
    // We do not know if this fileId is ours or someone else's. We must check that.
    FileInfo own = this.ownFilesInfo.get(fileId);

    // It is our file.
    if (own != null) {
      return own.hasBackupPeer(chunkNumber, peerId);
    } else {
      // It is someone's else, possibly unknown.
      FileInfo other = this.otherFilesInfo.get(fileId);
      return (other != null) && other.hasBackupPeer(chunkNumber, peerId);
    }
  }

  /**
   * Assigns a new backup peer to the given chunk (identified by its number and the id
   * of the file to which it belongs).
   *
   * @param fileId      The file's id.
   * @param chunkNumber The chunk's number.
   * @param peerId      The new backup peer's id.
   */
  public void addBackupPeer(String fileId, Integer chunkNumber, Long peerId) {
    // We do not know if this fileId is ours or someone else's. We must check that.
    FileInfo own = this.ownFilesInfo.get(fileId);

    if (own != null) {
      // It is our file.
      own.addBackupPeer(chunkNumber, peerId);
    } else {
      // It is someone's else, possibly new.
      FileInfo other = this.otherFilesInfo.computeIfAbsent(fileId, FileInfo::new);
      other.addBackupPeer(chunkNumber, peerId);
    }
  }

  /**
   * Removes a backup peer from the given chunk (identified by its number and the id of
   * the file to which it belongs).
   *
   * @param fileId      The file's id.
   * @param chunkNumber The chunk's number.
   * @param peerId      The id of the peer which used to have a backup of that chunk.
   */
  public void removeBackupPeer(String fileId, Integer chunkNumber, Long peerId) {
    // We do not know if this fileId is ours or someone else's. We must check that.
    FileInfo own = this.ownFilesInfo.get(fileId);

    // It is our file.
    if (own != null) {
      own.removeBackupPeer(chunkNumber, peerId);
    } else {
      // It is someone's else, possibly unknown.
      FileInfo other = this.otherFilesInfo.get(fileId);
      if (other != null) other.removeBackupPeer(chunkNumber, peerId);
    }
  }

  /**
   * Removes a backup peer from the chunk of the given file
   *
   * @param fileId The file's id.
   * @param peerId The id of the peer which could have had a backup of a chunk of the
   *               given file.
   */
  public void removeBackupPeer(String fileId, Long peerId) {
    // We do not know if this fileId is ours or someone else's. We must check that.
    FileInfo own = this.ownFilesInfo.get(fileId);

    // It is our file.
    if (own != null) {
      own.removeBackupPeer(peerId);
    } else {
      // It is someone's else, possibly unknown.
      FileInfo other = this.otherFilesInfo.get(fileId);
      if (other != null) other.removeBackupPeer(peerId);
    }
  }

  /**
   * Checks if any of the file's chunks has a backup on other peers
   *
   * @param fileId The file's id.
   * @return True if any of the file's chunks has a backup on other peers and false
   * otherwise.
   */
  public boolean hasBackupPeers(String fileId) {
    FileInfo info = this.otherFilesInfo.get(fileId);
    return info != null && info.hasBackupPeers();
  }

  public void storeState() {
    FilesManager.getInstance().writeOwnFilesInfo(ownFilesInfo);
    FilesManager.getInstance().writeOtherFilesInfo(otherFilesInfo);
  }

  /**
   * Constructs a string that contains the state of this peer's filesystem.
   */
  public String dumpState() {
    StringBuilder string = new StringBuilder();
    for (Map.Entry<String,OwnFileInfo> entry : ownFilesInfo.entrySet()) {
      string.append(entry.getValue().toString());
    }
    for (Map.Entry<String,FileInfo> entry : otherFilesInfo.entrySet()) {
      string.append(entry.getValue().toString());
    }
    long space = FilesManager.getInstance().backupTotalSpace();
    string.append("Total used backup space: ").append(space);
    return string.toString();
  }
}
