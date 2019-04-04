package dbs;

import java.io.File;
import java.security.MessageDigest;

public class Utils {

    private static final int leadingSpaces = 25;

    public static void printErr(String errorClass, String errorDescription) {
        System.err.print(String.format("%-" + leadingSpaces + "s", errorClass + ":"));
        System.err.println(errorDescription);
    }

    public static void printInfo(String infoClass, String infoDescription) {
        System.out.print(String.format("%-" + leadingSpaces + "s", infoClass + ":"));
        System.out.println(infoDescription);
    }

    public static String hash(File file, long peerId) throws Exception {
        String filePath = file.getPath();
        long lastModified = file.lastModified();
        Utils.printInfo("Utils", "pathname: " + filePath + " last: " + lastModified + peerId);
        String bitString = filePath + lastModified;
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] encodedHash;
        try {
            encodedHash = digest.digest(bitString.getBytes());
        } catch(Exception e) {
            Utils.printErr("Utils", "Could not execute hash function using the bit string '" + bitString + "'");
            return null;
        }
        return encodedHash.toString();
    }

    public static String getChunksReplicationDegreePathName(long peerId) {
        return Configuration.chunksReplicationDegreePathName + peerId + ".ser";
    }
}