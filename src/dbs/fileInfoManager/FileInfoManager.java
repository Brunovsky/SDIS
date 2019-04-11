package dbs.fileInfoManager;

import dbs.Peer;
import dbs.files.FilesManager;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class FileInfoManager {

  private static FileInfoManager manager;

  /**
   * Maps the id of a file to the information of that file.
   * This is useful in order to keep track of the actual number of peer's which back up
   * the same files has the owner of the FileInfoManager object (a peer), and to
   * compare it
   * against the desired replication degree of those chunks. The information regarding
   * the files
   * owned by that same peer may also be tracked.
   */
  private final ConcurrentHashMap<String,FileInfo> filesInfo;

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
    this.filesInfo = FilesManager.createInstance().initFilesInfo();
  }

  /**
   * Returns true if the map contains an entry for the given file id and false otherwise.
   *
   * @param fileId The file's id.
   * @return True if the map contains an entry for the given chunk and false otherwise.
   */
  public boolean hasFileInfo(String fileId) {
    return this.filesInfo.containsKey(fileId);
  }

  /**
   * Returns the information regarding the specified file if it exists.
   *
   * @param fileId The file's id.
   * @return The information regarding the specified file or null if that information
   * doesn't exist.
   */
  private FileInfo getFileInfo(String fileId) {
    return this.filesInfo.get(fileId);
  }

  /**
   * Returns true if the given chunk is backed up by that peer and false otherwise.
   *
   * @param fileId      The file's id.
   * @param chunkNumber The chunk's number.
   * @return True if the given chunk is backed up by that peer and false otherwise.
   */
  public boolean hasChunk(String fileId, Integer chunkNumber) {
    return FilesManager.getInstance().hasChunk(fileId, chunkNumber);
  }

  /**
   * Stores a new chunk. If another chunk with the same name exists, it will be
   * overwritten.
   *
   * @param fileId      The file's id
   * @param chunkNumber The chunk's number
   * @param chunk       The chunk's content
   */
  public void storeChunk(String fileId, Integer chunkNumber, byte[] chunk) {
    if (this.hasChunk(fileId, chunkNumber)) return;  // chunk already stored
    FilesManager.getInstance().putChunk(fileId, chunkNumber, chunk);
  }

  /**
   * Returns the content of a chunk, or null if it does not exist or a reading error
   * occurred.
   *
   * @param fileId      The file's id
   * @param chunkNumber The chunk's number
   * @return The entire content of the chunk, or null if the chunk does not exist/could
   * not be read.
   */
  public byte[] getChunk(String fileId, Integer chunkNumber) {
    return FilesManager.getInstance().getChunk(fileId, chunkNumber);
  }

  /**
   * Adds a new entry to the filesInfo map if it doesn't exist yet.
   *
   * @param fileId The id of the new file.
   */
  public void addFileInfo(String fileId) {
    if (!this.hasFileInfo(fileId))
      this.filesInfo.put(fileId, new FileInfo());
  }

  /**
   * Removes the entry from the filesInfo map that corresponds to given file's id.
   *
   * @param fileId The id of the file to be deletes from the records.
   */
  public void deleteFileInfo(String fileId) {
    this.filesInfo.remove(fileId);
  }

  /**
   * Deletes the provided file and its records.
   *
   * @param file   The file to delete.
   * @param fileId The file's id.
   * @return True if the provided file and its records were successfully deleted and
   * false otherwise.
   */
  public boolean deleteFile(File file, String fileId) {
    if (FilesManager.deleteRecursive(file))
      Peer.log("Successfully deleted file " + file.getPath() + " and its records",
          Level.INFO);
    else {
      Peer.log("Could not delete file " + file.getPath() + " and its records",
          Level.SEVERE);
      return false;
    }
    FilesManager.getInstance().deleteFileInfo(fileId);
    return true;
  }

  /**
   * Deletes a backed up file and its records.
   *
   * @param fileId The file's id.
   * @return True if the backed up file and its records were successfully deleted and
   * false otherwise.
   */
  public boolean deleteBackedUpFile(String fileId) {
    if (!this.hasFileInfo(fileId)) return true;
    if (!FilesManager.getInstance().deleteBackupFile(fileId)) {
      Peer.log("Could not delete the backup folder for the file with id " + fileId,
          Level.SEVERE);
      return false;
    }
    this.deleteFileInfo(fileId);
    if (!FilesManager.getInstance().deleteFileInfo(fileId)) {
      Peer.log("Could not delete the records for the file with id " + fileId,
          Level.SEVERE);
      return false;
    }
    return true;
  }

  /**
   * Updates the desired replication degree of a file
   *
   * @param fileId                   The file's id.
   * @param desiredReplicationDegree The desired replication degree of that file.
   */
  public void setDesiredReplicationDegree(String fileId,
                                          Integer desiredReplicationDegree) {
    if (!this.hasFileInfo(fileId))
      this.addFileInfo(fileId);
    this.getFileInfo(fileId).setDesiredReplicationDegree(desiredReplicationDegree);
    this.writeDesiredReplicationDegree(fileId);
  }

  /**
   * Adds a new chunk to the given file.
   *
   * @param fileId      The id of the file to which a new chunk is being added.
   * @param chunkNumber The number of the new chunk of the given file.
   */
  public void addFileChunk(String fileId, Integer chunkNumber) {
    if (!this.hasFileInfo(fileId))
      this.addFileInfo(fileId);
    this.getFileInfo(fileId).addFileChunk(chunkNumber);
    this.writeChunkInfoFile(fileId, chunkNumber);
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
    if (!this.hasFileInfo(fileId))
      this.addFileInfo(fileId);
    this.getFileInfo(fileId).addBackupPeer(chunkNumber, peerId);
    this.writeChunkInfoFile(fileId, chunkNumber);
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
    if (!this.hasFileInfo(fileId)) return;
    this.getFileInfo(fileId).removeBackupPeer(chunkNumber, peerId);
    this.writeChunkInfoFile(fileId, chunkNumber);
  }

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
    if (!this.hasFileInfo(fileId)) return false;
    return this.getFileInfo(fileId).hasBackupPeer(chunkNumber, peerId);
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
    if (!this.hasFileInfo(fileId)) return 0;
    return this.getFileInfo(fileId).getChunkReplicationDegree(chunkNumber);
  }

  /**
   * Returns the desired replication degree of the given file or null if that file is
   * unknown to the peer.
   *
   * @param fileId The file's id.
   * @return The desired replication degree of the given file or null if that file is
   * unknown to the peer.
   */
  public Integer getDesiredReplicationDegree(String fileId) {
    if (!this.hasFileInfo(fileId)) return null;
    return this.getFileInfo(fileId).getDesiredReplicationDegree();
  }

  /**
   * Allows for the update of a given chunk info on the disk (filesinfo folder)
   *
   * @param fileId      The id of the file to which that chunk belongs.
   * @param chunkNumber The chunk's number.
   */
  private void writeChunkInfoFile(String fileId, Integer chunkNumber) {
    ChunkInfo chunkInfo = this.getFileInfo(fileId).getChunkInfo(chunkNumber);
    if (chunkInfo == null) return;
    try {
      FilesManager.getInstance().writeChunkInfo(fileId, chunkNumber, chunkInfo);
    } catch (Exception e) {
      Peer.log("Could not update the chunk info for the chunk number " + chunkNumber +
          " of the file with id " + fileId, Level.SEVERE);
    }
  }

  /**
   * Allows for the update of the desired replication degree of a given file on the disk
   *
   * @param fileId The file's id
   */
  private void writeDesiredReplicationDegree(String fileId) {
    Integer desiredReplicationDegree =
        this.getFileInfo(fileId).getDesiredReplicationDegree();
    try {
      FilesManager.getInstance().writeFileDesiredReplicationDegree(fileId,
          desiredReplicationDegree);
    } catch (Exception e) {
      Peer.log("Could not update the desired replication degree of the file with id " + fileId, Level.SEVERE);
    }
  }

  public boolean putRestore(String filename, byte[][] chunks) {
    return FilesManager.getInstance().putRestore(filename, chunks);
  }

  public boolean putChunk(String fileId, Integer chunkNumber, byte[] chunk) {
    return FilesManager.getInstance().putChunk(fileId, chunkNumber, chunk);
  }

  public boolean deleteChunk(String fileId, Integer chunkNumber) {
    return FilesManager.getInstance().deleteChunk(fileId, chunkNumber);
  }
}
