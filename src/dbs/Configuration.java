package dbs;

public class Configuration {

    // Which protocol version does this peer use?
    public static String version = "1.0";

    // Folder where each peer root directory is stored.
    public static String allPeersRootDir = "/tmp/dbs";

    // Peer's root directory.
    // TODO: Define a prefix and use "peer-$PEERID/" or use "peer/$PEERID/" ?
    public static String peerRootDirPrefix = "peer-";

    // Backup subdirectory, where chunks kept by this peer are kept.
    // Note: This might be inlined somewhere already
    public static String backupDir = "backup";

    // Backup entry prefix
    // TODO: Define a prefix and use "prefix-$CHUNKNO" or use simply $CHUNKNO ?
    public static String entryPrefix = "file-";

    // Chunk file prefix.
    // TODO: Define a prefix and use "prefix-$CHUNKNO" or use simply $CHUNKNO ?
    public static String chunkPrefix = "chunk-";

    // Restore subdirectory, where files restored are kept.
    public static String restoredDir = "restored";

    // My files' id mapping directory
    public static String idMapDir = "idmap";

    // My files' metadata directory
    public static String chunkInfoDir = "mine";

    // My file's info (desired replication degree and set of peers which have a backup of those files' chunks)
    public static String filesinfoDir = "filesinfo";

    // My file's desired replication degree file name
    public static String desiredReplicationDegreeFile = "drd";

    // Multicaster's timeout for reading from multicast socket
    public static int multicastTimeout = 300; // milliseconds

    // Peer socket's timeout for waiting on new queue message
    public static int socketTimeout = 300; // milliseconds

    // Peer socket's message queue capacity, (in datagram packets)
    public static int socketQueueCapacity = 10000;

    // Thread pool sizes (core pool sizes)
    public static int peerThreadPoolSize = 8;
    public static int putchunkPoolSize = 10;
    public static int storedPoolSize = 25;
    public static int chunkPoolSize = 35;
    public static int getChunkPoolSize = 15;
    public static int restorerPoolSize = 5;
    public static int removedPoolSize = 25;

    // Maximum number of allowed GETCHUNK for each chunk before the restore gives up
    public static int maxGetchunkAttempts = 5;

    // Maximum storage capacity for chunks.
    public static long storageCapacity = 40000; // KB ?
}
