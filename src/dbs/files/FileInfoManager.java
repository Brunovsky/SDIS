package dbs.files;

import dbs.ChunkKey;
import dbs.Configuration;
import dbs.Peer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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

  private final AtomicLong usedSpace = new AtomicLong(0);

  private Set<ChunkKey> missingChunks;

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

    cleanup();
    usedSpace.set(FilesManager.getInstance().backupTotalSpace());

    // Populate pathname Map.
    this.pathnameMap = new ConcurrentHashMap<>();
    for (Map.Entry<String,OwnFileInfo> entry : ownFilesInfo.entrySet()) {
      OwnFileInfo info = entry.getValue();
      pathnameMap.put(info.getPathname(), info);
    }

    Runtime.getRuntime().addShutdownHook(new Thread(this::storeState));
  }

  /**
   * There might be disagreement between the metadata files and the actual backed up
   * chunks files we are keeping from other peers.
   *
   * We must consider: whether we have Chunk(K, N) in backup/K/N (we have the data) and
   * whether we have Chunk(K, N) in otherFilesInfo (we have the metadata)
   *
   * Case 1.
   *   We have the data and the metadata. Great, nothing to do here.
   *
   * Case 2.
   *   We have the data but not the metadata. Delete the data and move on, reclaiming
   *   unused disk space. This philosophy follows from the assumption that the metadata
   *   in each peer is never lost.
   *
   * Case 3.
   *   We have the metadata but not the data. Add this Chunk(K, N) to a passively
   *   removed list, to be later handled by the Peer sending 'Removed' messages for
   *   this chunks to the network. This might start a Putchunk that brings the chunks
   *   back to this peer.
   */
  private void cleanup() {
    missingChunks = new HashSet<>();

    findMissingMetadata();

    findMissingData();
  }

  private void findMissingMetadata() {
    FilesManager manager = FilesManager.getInstance();
    HashMap<String,TreeSet<Integer>> chunkData = manager.backupAllChunksMap();

    // Handle case 2
    for (Map.Entry<String,TreeSet<Integer>> entry : chunkData.entrySet()) {
      String fileId = entry.getKey();
      TreeSet<Integer> numbers = entry.getValue();

      FileInfo info = otherFilesInfo.get(fileId);

      if (info == null) {
        // Add all chunks to the missingChunks set
        for (Integer chunkNumber : numbers) {
          missingChunks.add(new ChunkKey(fileId, chunkNumber));
        }
      } else {
        // Check each chunk number
        for (Integer chunkNumber : numbers) {
          if (!info.hasChunk(chunkNumber)) {
            missingChunks.add(new ChunkKey(fileId, chunkNumber));
          }
        }
      }
    }
  }

  private void findMissingData() {
    FilesManager manager = FilesManager.getInstance();

    Set<Map.Entry<String,FileInfo>> filesInfoSet = otherFilesInfo.entrySet();
    Iterator<Map.Entry<String,FileInfo>> filesIterator = filesInfoSet.iterator();

    while (filesIterator.hasNext()) {
      Map.Entry<String,FileInfo> file = filesIterator.next();
      String fileId = file.getKey();
      FileInfo fileInfo = file.getValue();

      if (!manager.hasBackupFolder(fileId)) {
        filesIterator.remove();
      } else {
        Set<Integer> chunkSet = fileInfo.fileChunks.keySet();
        chunkSet.removeIf(chunkNumber -> !manager.hasChunk(fileId, chunkNumber));
      }
    }
  }

  public Set<ChunkKey> getMissingChunks() {
    Set<ChunkKey> missing = this.missingChunks;
    this.missingChunks = null;
    return missing;
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
    OwnFileInfo info = new OwnFileInfo(pathname, fileId, numberOfChunks, desired);

    synchronized (pathnameMap) {
      this.ownFilesInfo.put(fileId, info);
      pathnameMap.put(pathname, info);
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

  public void deleteOwnFileInfo(String fileId) {
    synchronized (pathnameMap) {
      OwnFileInfo info = this.ownFilesInfo.remove(fileId);
      if (info != null) pathnameMap.remove(info.getPathname());
    }
  }

  public void deleteOtherFile(String fileId) {
    FileInfo info = otherFilesInfo.get(fileId);
    if (info == null) return;

    for (ChunkInfo chunkInfo : info.fileChunks.values()) {
      deleteChunk(fileId, chunkInfo.getChunkNumber());
    }
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
   * Stores a new chunk. If the chunk already exists, we continue.
   *
   * @param fileId      The file's id
   * @param chunkNumber The chunk's number
   * @param chunk       The chunk's content
   */
  public boolean storeChunk(String fileId, Integer chunkNumber, byte[] chunk) {
    FileInfo info = this.otherFilesInfo.computeIfAbsent(fileId, FileInfo::new);
    if (FilesManager.getInstance().hasChunk(fileId, chunkNumber)) return true;

    // NOTE: Annoying race here... ...
    long size = FilesManager.getInstance().backupChunkTotalSpace(fileId, chunkNumber);
    long increment = chunk.length - (size == -1 ? 0 : size);

    if (increment > 0) {
      long newTotal = usedSpace.addAndGet(increment);
      if (newTotal > Configuration.storageCapacityKB * 1000) {
        usedSpace.addAndGet(-increment);
        return false;
      }
    }

    info.addChunkInfo(chunkNumber);
    info.addBackupPeer(chunkNumber, Peer.getInstance().getId());
    FilesManager.getInstance().putChunk(fileId, chunkNumber, chunk);
    return true;
  }

  /**
   * Deletes a stored chunk.
   *
   * @param fileId      The file's id
   * @param chunkNumber The chunk's number
   */
  public void deleteChunk(String fileId, Integer chunkNumber) {
    FileInfo info = this.otherFilesInfo.get(fileId);
    if (info == null) return;
    info.removeBackupPeer(chunkNumber, Peer.getInstance().getId());

    long size = FilesManager.getInstance().backupChunkTotalSpace(fileId, chunkNumber);
    if (size == -1) return;
    usedSpace.addAndGet(-size);
    FilesManager.getInstance().deleteChunk(fileId, chunkNumber);
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

  // ***** Methods concerning storage capacity
  public synchronized TreeSet<ChunkKey> trimBackup() {
    long maxDiskSpaceKB = Configuration.storageCapacityKB;
    long maxDiskSpace = maxDiskSpaceKB * 1000; // 9000000
    long used = usedSpace.get(); // 10000000
    long usedKB = used / 1000;

    System.out.println("maxDiskSpace: " + maxDiskSpace);
    System.out.println("used: " + used);

    // The used storage is lower than the maximum!
    if (used <= maxDiskSpace) {
      Peer.log("Peer is using " + usedKB + "KB of backup space, which is already " +
          "lower than the capacity " + maxDiskSpaceKB + "KB. Therefore no files need to" +
          " be removed just yet", Level.INFO);
      return null;
    }

    // Get a map of all chunks.
    TreeSet<ChunkInfo> set = getChunkSet();
    TreeSet<ChunkKey> filtered = new TreeSet<>();

    long recoverTotal = used - maxDiskSpace;
    long total = 0;

    for (ChunkInfo chunkInfo : set) {
      String fileId = chunkInfo.getFileId();
      int chunkNumber = chunkInfo.getChunkNumber();
      long length = FilesManager.getInstance().backupChunkTotalSpace(fileId, chunkNumber);

      total += length;
      filtered.add(chunkInfo.getKey());

      // Check if we have selected enough chunks for removal.
      if (total >= recoverTotal) break;
    }

    long totalKB = total / 1000;

    Peer.log("Removing " + filtered.size() + " external chunks to reclaim memory space." +
            " Recovered " + totalKB + "KB total memory", Level.INFO);

    return filtered;
  }

  private TreeSet<ChunkInfo> getChunkSet() {
    TreeSet<ChunkInfo> set = new TreeSet<>(new ChunkInfo.ComparisonReplication());

    for (FileInfo file : otherFilesInfo.values()) {
      set.addAll(file.fileChunks.values());
    }

    System.out.println("Set size: " + set.size());

    return set;
  }

  private void storeState() {
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
    long space = usedSpace.get();
    string.append("Total used backup space: ");
    string.append(space / 1000).append(" KB\n");
    string.append("Total allowed storage space for backup subsystem: ");
    string.append(Configuration.storageCapacityKB).append(" KB\n");
    return string.toString();
  }
}
