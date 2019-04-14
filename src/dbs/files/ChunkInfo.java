package dbs.files;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ChunkInfo implements Serializable {

  private Integer chunkNumber;

  /**
   * Set of peers which have a backup of that chunk.
   */
  private Set<Long> backupPeers;

  /**
   * Constructs a new object of the ChunkInfo class.
   */
  ChunkInfo(int chunkNumber) {
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
   * Returns this chunk's number. Used as key for the file's chunk map.
   *
   * @return This chunk's number.
   */
  int getChunkNumber() {
    return chunkNumber;
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
        backupPeers.equals(chunkInfo.backupPeers);
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
}
