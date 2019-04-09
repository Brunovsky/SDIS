package dbs.chunkManager;

import java.util.HashMap;

public class FileInfoManager {

  /**
   * Maps the id of a file to the information of that file.
   * This is useful in order to keep track of the actual number of peer's which back up
   * the same files has the owner of the FileInfoManager object (a peer), and to compare it
   * against the desired replication degree of those chunks. The information regarding the files
   * owned by that same peer may also be tracked.
   */
  private HashMap<String, FileInfo> filesInfo;

  /**
   * Constructs a new object of the FileInfoManager class.
   */
  public FileInfoManager() {
    this.filesInfo = new HashMap<>();
  }

  /**
   * Returns true if the map contains an entry for the given file id and false otherwise.
   * @param fileId The file's id.
   * @return True if the map contains an entry for the given chunk and false otherwise.
   */
  private boolean hasFileInfo(String fileId) {
    return this.filesInfo.containsKey(fileId);
  }

  /**
   * Returns the information regarding the specified file if it exists.
   * @param fileId The file's id.
   * @return The information regarding the specified file or null if that information doesn't exist.
   */
  private FileInfo getFileInfo(String fileId) {
    return this.filesInfo.get(fileId);
  }

  /**
   * Adds a new entry to the filesInfo map if it doesn't exist yet.
   * @param fileId The id of the new file.
   */
  public void addFileInfo(String fileId) {
    if(!this.hasFileInfo(fileId))
      this.filesInfo.put(fileId, new FileInfo());
  }

  /**
   * Adds a new chunk to the given file.
   * @param fileId The id of the file to which a new chunk is being added.
   * @param chunkNumber The number of the new chunk of the given file.
   */
  public void addFileChunk(String fileId, Integer chunkNumber) {
    if(!this.hasFileInfo(fileId))
      this.addFileInfo(fileId);
    this.getFileInfo(fileId).addFileChunk(chunkNumber);
  }

  /**
   * Assigns a new backup peer to the given chunk (identified by its number and the id of the file to which it belongs).
   * @param fileId The file's id.
   * @param chunkNumber The chunk's number.
   * @param peerId The new backup peer's id.
   */
  public void addBackupPeer(String fileId, Integer chunkNumber, Long peerId) {
    if(!this.hasFileInfo(fileId))
      this.addFileInfo(fileId);
    this.getFileInfo(fileId).addBackupPeer(chunkNumber, peerId);
  }

  /**
   * Removes a backup peer from the given chunk (identified by its number and the id of the file to which it belongs).
   * @param fileId The file's id.
   * @param chunkNumber The chunk's number.
   * @param peerId The id of the peer which used to have a backup of that chunk.
   */
  public void removeBackupPeer(String fileId, Integer chunkNumber, Long peerId) {
    if(!this.hasFileInfo(fileId)) return;
    this.getFileInfo(fileId).removeBackupPeer(chunkNumber, peerId);
  }

  /**
   * Returns true if the given peer has a backup of the given chunk (identified by its number and the id of the file to which it belongs).
   * @param fileId The file's id.
   * @param chunkNumber The chunk's number.
   * @param peerId The peer's id.
   * @return True if the given peer has a backup of the given chunk.
   */
  public boolean hasBackupPeer(String fileId, Integer chunkNumber, Long peerId) {
    if(!this.hasFileInfo(fileId)) return false;
    return this.getFileInfo(fileId).hasBackupPeer(chunkNumber, peerId);
  }

  /**
   * Returns the number of peer's which have a backup of the given chunk (the actual replication degree of the chunk).
   * @param fileId The id of the file to which that chunk belongs.
   * @param chunkNumber The chunk's number.
   * @return The number of peer's which have a backup of that chunk (the actual replication degree of the chunk).
   */
  public int getChunkReplicationDegree(String fileId, Integer chunkNumber) {
    if(!this.hasFileInfo(fileId)) return 0;
    return this.getFileInfo(fileId).getChunkReplicationDegree(chunkNumber);
  }

  /**
   * Returns the desired replication degree of the given file or null if that file is unknown to the peer.
   * @param fileId The file's id.
   * @return The desired replication degree of the given file or null if that file is unknown to the peer.
   */
  public int getDesiredReplicationDegree(String fileId) {
    if(!this.hasFileInfo(fileId)) return null;
    return this.getFileInfo(fileId).getDesiredReplicationDegree();
  }
}
