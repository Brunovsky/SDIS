package dbs;

import java.io.File;
import java.security.MessageDigest;
import java.util.logging.Logger;

public class Utils {
  private final static Logger LOGGER = Logger.getLogger(Utils.class.getName());

  public static byte[] hash(File file, long peerId) throws Exception {
    String filePath = file.getPath();
    long lastModified = file.lastModified();
    String bitString = filePath + lastModified;
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] encodedHash;
    try {
      encodedHash = digest.digest(bitString.getBytes());
    } catch (Exception e) {
      LOGGER.severe("Could not execute hash function using the bit string '" + bitString + "'\n");
      return null;
    }
    return encodedHash;
  }

  public static String getChunksReplicationDegreePathName(long peerId) {
    return Configuration.chunksReplicationDegreePathName + peerId + ".ser";
  }
}