package dbs;

import java.io.File;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.security.MessageDigest;
import java.util.Random;

public class Utils {
  private static String bytesToHex(byte[] hash) {
    StringBuffer hexString = new StringBuffer();
    for (int i = 0; i < hash.length; i++) {
      String hex = Integer.toHexString(0xff & hash[i]);
      if (hex.length() == 1) hexString.append('0');
      hexString.append(hex);
    }
    return hexString.toString();
  }

  public static String hash(File file, long peerId) throws Exception {
    String filePath = file.getPath();
    long lastModified = file.lastModified();
    String bitString = filePath + lastModified + peerId;
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] encodedHash = digest.digest(bitString.getBytes());
    return bytesToHex(encodedHash);
  }

  public static int numberOfChunks(long filesize) {
    return (int) ((filesize + Protocol.chunkSize) / Protocol.chunkSize);
  }

  public static Registry registry() {
    Registry registry;

    try {
      // Try to get the already open registry.
      registry = LocateRegistry.getRegistry("localhost");
    } catch (RemoteException e1) {
      try {
        // Race: There is no open registry. Create one.
        // TODO: where to find selected port?
        registry = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
      } catch (RemoteException e2) {
        // Lost race: someone created a registry in the meanwhile.
        try {
          registry = LocateRegistry.getRegistry("localhost");
        } catch (RemoteException e3) {
          // Something very bad happened.
          throw new Error(e1);  // throw the first error
        }
      }
    }

    return registry;
  }

  public static int getRandom(int min, int max) {
    Random rand = new Random();  // TODO: make static
    return rand.nextInt(max - min + 1) + min;
  }

  public static boolean validVersion(String version) {
    return version.matches("[0-9]\\.[0-9]");
  }

  public static boolean validSenderId(String senderId) {
    return senderId.matches("[0-9]+");
  }

  public static boolean validFileId(String fileId) {
    return fileId.length() == 64 && fileId.matches("[a-fA-F0-9]+");
  }

  public static boolean validChunkNo(String chunkNo) {
    return chunkNo.matches("[0-9]+");
  }

  public static boolean validChunkNo(int chunkNo) {
    return chunkNo >= 0;
  }

  public static boolean validReplicationDegree(String replication) {
    return replication.matches("[0-9]");
  }

  public static boolean validReplicationDegree(int replication) {
    return replication >= 0 && replication <= 9;
  }
}
