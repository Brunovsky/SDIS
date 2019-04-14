package dbs.files;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class FileInfo implements Serializable {

  private final String fileId;

  /**
   * Maps the number of a file's chunk (greater or equal to 0) to the information of
   * that chunk.
   */
  private final ConcurrentHashMap<Integer,ChunkInfo> fileChunks;

  /**
   * The desired replication degree of that file.
   */
  private Integer desiredReplicationDegree;

  /**
   * Constructs a new object of the FileInfo class.
   */
  FileInfo(String fileId) {
    this.fileId = fileId;
    this.fileChunks = new ConcurrentHashMap<>();
    this.desiredReplicationDegree = 0;
  }

  /**
   * Constructs a new object of the FileInfo class, given the desired replication
   * degree for that file.
   *
   * @param desiredReplicationDegree The desired replication degree for that file.
   */
  FileInfo(String fileId, Integer desiredReplicationDegree) {
    this.fileId = fileId;
    this.fileChunks = new ConcurrentHashMap<>();
    this.desiredReplicationDegree = desiredReplicationDegree;
  }

  /**
   * Returns true if the map contains an entry for the given chunk and false otherwise.
   *
   * @param chunkNumber The chunk's number.
   * @return True if the map contains an entry for the given chunk and false otherwise.
   */
  public boolean hasChunk(Integer chunkNumber) {
    return this.fileChunks.containsKey(chunkNumber);
  }

  /**
   * Returns the information regarding the specified chunk if it exists.
   *
   * @param chunkNumber The chunk's number.
   * @return The information regarding the specified chunk or null if that information
   * doesn't exist.
   */
  ChunkInfo getChunkInfo(Integer chunkNumber) {
    return this.fileChunks.get(chunkNumber);
  }

  /**
   * Adds a new entry to the fileChunks map if it doesn't exist yet.
   *
   * @param chunkNumber The number of the new chunk of that file.
   */
  ChunkInfo addChunkInfo(Integer chunkNumber) {
    return this.fileChunks.computeIfAbsent(chunkNumber, ChunkInfo::new);
  }

  /**
   * Adds the id of the new peer which has a backup of that chunk.
   *
   * @param chunkNumber The number of the chunk being backed up.
   * @param peerId      The id of the new peer to backup that chunk.
   * @return the new perceived replication degree of the chunk.
   */
  void addBackupPeer(Integer chunkNumber, Long peerId) {
    this.addChunkInfo(chunkNumber).addBackupPeer(peerId);
  }

  /**
   * Removes the id of the peer which could have had a backup of that chunk.
   *
   * @param chunkNumber The number of the chunk that is no longer being backed up by the
   *                    given peer.
   * @param peerId      The id of the removing peer.
   * @return the new perceived replication degree of the chunk.
   */
  void removeBackupPeer(Integer chunkNumber, Long peerId) {
    this.getChunkInfo(chunkNumber).removeBackupPeer(peerId);
  }

  /**
   * Removes the id of the peer which could have had a backup of a chunk of the given file
   *
   * @param peerId The id of the peer which could have had a backup of a chunk of the
   *               given file
   */
  void removeBackupPeer(Long peerId) {
    for (Map.Entry<Integer,ChunkInfo> integerChunkInfoEntry :
        this.fileChunks.entrySet()) {
      this.removeBackupPeer(integerChunkInfoEntry.getKey(), peerId);
    }
  }

  /**
   * Returns true if the given peer has a backup of that chunk and false otherwise.
   *
   * @param chunkNumber The chunk's number.
   * @param peerId      The peer's id.
   * @return True if the given peer has a backup of that chunk and false otherwise.
   */
  public boolean hasBackupPeer(Integer chunkNumber, Long peerId) {
    ChunkInfo chunkInfo = this.getChunkInfo(chunkNumber);
    return chunkInfo != null && chunkInfo.hasBackupPeer(peerId);
  }

  /**
   * Checks if any of the file's chunks has a backup on other peers
   *
   * @return True if any of the file's chunks has a backup on other peers and false
   * otherwise.
   */
  public boolean hasBackupPeers() {
    Iterator it = this.fileChunks.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry) it.next();
      if (((ChunkInfo) pair.getValue()).hasBackupPeers())
        return true;
    }
    return false;
  }

  /**
   * Returns the number of peer's which have a backup of that chunk (the actual
   * replication degree of the chunk).
   *
   * @param chunkNumber The chunk's number.
   * @return The number of peer's which have a backup of that chunk (the actual
   * replication degree of the chunk).
   */
  public int getChunkReplicationDegree(Integer chunkNumber) {
    ChunkInfo chunkInfo = this.getChunkInfo(chunkNumber);
    return chunkInfo == null ? 0 : chunkInfo.getReplicationDegree();
  }

  /**
   * Returns the desired replication degree of that file.
   *
   * @return The desired replication degree of that file.
   */
  public int getDesiredReplicationDegree() {
    return desiredReplicationDegree;
  }

  /**
   * Sets the desired replication degree of that file.
   *
   * @param desiredReplicationDegree The desired replication degree of that file.
   */
  public void setDesiredReplicationDegree(Integer desiredReplicationDegree) {
    this.desiredReplicationDegree = desiredReplicationDegree;
  }

  /**
   * Get this file's id
   * @return this file's id
   */
  public String getFileId() {
    return this.fileId;
  }

  @Override
  public String toString() {
    StringBuilder string = new StringBuilder();
    string.append(' ').append(fileId).append('\n');
    string.append("   Desired Replication Degree: ");
    string.append(desiredReplicationDegree).append('\n');
    for (Map.Entry<Integer, ChunkInfo> entry : this.fileChunks.entrySet()) {
      string.append("     ").append(entry.getValue().toString()).append('\n');
    }
    return string.toString();
  }
}
