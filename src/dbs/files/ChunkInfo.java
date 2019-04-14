package dbs.files;

import dbs.ChunkKey;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ChunkInfo implements Serializable, Comparable<ChunkInfo> {

  private final FileInfo fileInfo;
  private final Integer chunkNumber;

  /**
   * Set of peers which have a backup of that chunk.
   */
  private final Set<Long> backupPeers;

  /**
   * Constructs a new object of the ChunkInfo class.
   */
  ChunkInfo(FileInfo fileInfo, int chunkNumber) {
    this.fileInfo = fileInfo;
    this.chunkNumber = chunkNumber;
    this.backupPeers = ConcurrentHashMap.newKeySet();
  }

  /**
   * Returns true if the given peer has a backup of that chunk and false otherwise.
   *
   * @param peerId The peer's id.
   * @return True if the given peer has a backup of that chunk and false otherwise.
   */
  boolean hasBackupPeer(Long peerId) {
    return this.backupPeers.contains(peerId);
  }

  /**
   * Adds the id of the new peer which has a backup of that chunk.
   *
   * @param peerId The peer's id.
   */
  void addBackupPeer(Long peerId) {
    this.backupPeers.add(peerId);
  }

  /**
   * Removes the id of the peer which could have had a backup of that chunk.
   *
   * @param peerId The peer's id.
   */
  void removeBackupPeer(Long peerId) {
    this.backupPeers.remove(peerId);
  }

  FileInfo getFileInfo() {
    return this.fileInfo;
  }

  String getFileId() {
    return this.fileInfo.getFileId();
  }

  int getChunkNumber() {
    return chunkNumber;
  }

  ChunkKey getKey() {
    return new ChunkKey(fileInfo.getFileId(), chunkNumber);
  }

  /**
   * Returns the perceived number of peers which have a backup of this chunk (the actual
   * replication degree of the chunk).
   *
   * @return The number of peers which have a backup of this chunk (the actual
   * replication degree of the chunk).
   */
  int getReplicationDegree() {
    return this.backupPeers.size();
  }

  /**
   * Returns true if the chunk has a backup on other peer and false otherwise.
   *
   * @return True if the chunk has a backup on other peer and false otherwise.
   */
  boolean hasBackupPeers() {
    return (this.getReplicationDegree() != 0);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ChunkInfo chunkInfo = (ChunkInfo) o;
    return chunkNumber.equals(chunkInfo.chunkNumber) &&
        getFileId().equals(chunkInfo.getFileId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(chunkNumber);
  }

  @Override
  public String toString() {
    StringBuilder string = new StringBuilder();
    string.append("chunk #").append(chunkNumber);
    string.append(" {").append(backupPeers.size()).append("} ");
    for (long peer : backupPeers) string.append(peer).append(' ');
    return string.toString();
  }

  @Override
  public int compareTo(ChunkInfo other) {
    assert other != null;
    if (this == other) return 0;
    return getKey().compareTo(other.getKey());
  }

  static class ComparisonReplication implements Comparator<ChunkInfo> {
    @Override
    public int compare(ChunkInfo lhs, ChunkInfo rhs) {
      if (lhs == rhs) return 0;

      int lhsPerceived = lhs.getReplicationDegree();
      int lhsDesired = lhs.fileInfo.getDesiredReplicationDegree();
      int rhsPerceived = rhs.getReplicationDegree();
      int rhsDesired = rhs.fileInfo.getDesiredReplicationDegree();

      int lhsDiff = lhsPerceived - lhsDesired;
      int rhsDiff = rhsPerceived - rhsDesired;

      if (lhsDiff != rhsDiff) return rhsDiff - lhsDiff;

      if (lhsPerceived != rhsPerceived) return rhsPerceived - lhsPerceived;

      return lhs.compareTo(rhs);
    }
  }
}
