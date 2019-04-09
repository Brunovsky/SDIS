package dbs.fileInfoManager;

import java.util.HashSet;
import java.util.Set;

public class ChunkInfo implements java.io.Serializable {

  /**
   * Set of peers which have a backup of that chunk.
   */
  private Set<Long> backupPeers;

  /**
   * Constructs a new object of the ChunkInfo class.
   */
  public ChunkInfo() {
    this.backupPeers = new HashSet<>();
  }

  /**
   * Adds the id of the new peer which has a backup of that chunk.
   * @param peerId The peer's id.
   */
  public void addBackupPeer(Long peerId) {
    this.backupPeers.add(peerId);
  }

  /**
   * Removes the id of the peer which could have had a backup of that chunk.
   * @param peerId The peer's id.
   */
  public void removeBackupPeer(Long peerId) {
    this.backupPeers.remove(peerId);
  }

  /**
   * Returns true if the given peer has a backup of that chunk and false otherwise.
   * @param peerId The peer's id.
   * @return True if the given peer has a backup of that chunk and false otherwise.
   */
  public boolean hasBackupPeer(Long peerId) {
    return this.backupPeers.contains(peerId);
  }

  /**
   * Returns the number of peer's which have a backup of that chunk (the actual replication degree of the chunk).
   * @return The number of peer's which have a backup of that chunk (the actual replication degree of the chunk).
   */
  public int getReplicationDegree() {
    return this.backupPeers.size();
  }


}
