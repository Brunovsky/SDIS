package dbs.fileInfoManager;

import java.util.concurrent.ConcurrentHashMap;

public class FileInfo {

  /**
   * Maps the number of a file's chunk (greater or equal to 0) to the information of
   * that chunk.
   */
  private ConcurrentHashMap<Integer,ChunkInfo> fileChunks;
  /**
   * The desired replication degree of that file.
   */
  private Integer desiredReplicationDegree;

  /**
   * Constructs a new object of the FileInfo class.
   */
  public FileInfo() {
    this.fileChunks = new ConcurrentHashMap<>();
    this.desiredReplicationDegree = 0;
  }

  /**
   * Constructs a new object of the FileInfo class, given the desired replication
   * degree for that file.
   *
   * @param desiredReplicationDegree The desired replication degree for that file.
   */
  public FileInfo(Integer desiredReplicationDegree) {
    this.fileChunks = new ConcurrentHashMap<>();
    this.desiredReplicationDegree = desiredReplicationDegree;
  }

  /**
   * Constructs a new object of the FileInfo class, given the number of chunks of that
   * file.
   *
   * @param numberOfChunks The number of chunks of that file.
   */
  public FileInfo(int numberOfChunks, Integer desiredReplicationDegree) {
    this();
    this.desiredReplicationDegree = desiredReplicationDegree;
    for (int i = 0; i < numberOfChunks; i++)
      this.fileChunks.put(i, new ChunkInfo());
  }

  /**
   * Returns true if the map contains an entry for the given chunk and false otherwise.
   *
   * @param chunkNumber The chunk's number.
   * @return True if the map contains an entry for the given chunk and false otherwise.
   */
  private boolean hasChunk(Integer chunkNumber) {
    return this.fileChunks.containsKey(chunkNumber);
  }

  /**
   * Returns the information regarding the specified chunk if it exists.
   *
   * @param chunkNumber The chunk's number.
   * @return The information regarding the specified chunk or null if that information
   * doesn't exist.
   */
  public ChunkInfo getChunkInfo(Integer chunkNumber) {
    return this.fileChunks.get(chunkNumber);
  }

  /**
   * Adds a new entry to the fileChunks map if it doesn't exist yet.
   *
   * @param chunkNumber The number of the new chunk of that file.
   */
  public void addFileChunk(Integer chunkNumber) {
    if (!this.hasChunk(chunkNumber))
      this.fileChunks.put(chunkNumber, new ChunkInfo());
  }

  /**
   * Adds a new entry to the fileChunks map if it doesn't exist yet.
   *
   * @param chunkNumber The number of the new chunk of that file.
   * @param chunkInfo   The information regarding that chunk.
   */
  public void addFileChunk(Integer chunkNumber, ChunkInfo chunkInfo) {
    if (!this.hasChunk(chunkNumber))
      this.fileChunks.put(chunkNumber, chunkInfo);
  }

  /**
   * Adds the id of the new peer which has a backup of that chunk.
   *
   * @param chunkNumber The number of the chunk being backed up.
   * @param peerId      The id of the new peer to backup that chunk.
   */
  public void addBackupPeer(Integer chunkNumber, Long peerId) {
    if (!this.hasChunk(chunkNumber))
      this.addFileChunk(chunkNumber);
    this.getChunkInfo(chunkNumber).addBackupPeer(peerId);
  }

  /**
   * Removes the id of the peer which could have had a backup of that chunk.
   *
   * @param chunkNumber The if of the chunk that is no longer being backed up by the
   *                    given peer.
   * @param peerId      The id of the peer which could have had a backup of that chunk.
   */
  public void removeBackupPeer(Integer chunkNumber, Long peerId) {
    if (!this.hasChunk(chunkNumber)) return;
    this.getChunkInfo(chunkNumber).removeBackupPeer(peerId);
  }

  /**
   * Returns true if the given peer has a backup of that chunk and false otherwise.
   *
   * @param chunkNumber The chunk's number.
   * @param peerId      The peer's id.
   * @return True if the given peer has a backup of that chunk and false otherwise.
   */
  public boolean hasBackupPeer(Integer chunkNumber, Long peerId) {
    if (!this.hasChunk(chunkNumber)) return false;
    return this.getChunkInfo(chunkNumber).hasBackupPeer(peerId);
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
    if (!this.hasChunk(chunkNumber)) return 0;
    return this.getChunkInfo(chunkNumber).getReplicationDegree();
  }

  /**
   * Returns the desired replication degree of that file.
   *
   * @return The desired replication degree of that file.
   */
  public Integer getDesiredReplicationDegree() {
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

  @Override
  public String toString() {
    StringBuilder string = new StringBuilder();
    string.append("  Replication Degree: ").append(desiredReplicationDegree).append('\n');
    for (Integer chunkNumber : fileChunks.keySet()) {
      ChunkInfo chunkinfo = fileChunks.get(chunkNumber);
      string.append("   Chunk ").append(chunkNumber).append(": ");
      string.append(chunkinfo.toString()).append('\n');
    }
    return string.toString();
  }
}
