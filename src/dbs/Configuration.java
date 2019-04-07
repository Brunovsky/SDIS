package dbs;

public class Configuration {
    // Which protocol version does this peer use?
    public String version = "1.0";

    // This peer's id. TODO: is this better here? (don't think so)
    //public String id;

    // This peer's access point. TODO: is this better here? (don't think so)
    //public String accessPoint;

    // Peer's root directory.
    // TODO: Define a prefix and use "peer-$PEERID/" or use "peer/$PEERID/" ?
    public String peerRootDirPrefix = "peer-";

    // Backup subdirectory, where chunks kept by this peer are kept.
    // Note: This might be inlined somewhere already
    public String backupDir = "backup";

    // Chunk file prefix.
    // TODO: Define a prefix and use "prefix-$CHUNKNO" or use simply $CHUNKNO ?
    //public String chunkPrefix;

    // Restore subdirectory, where files restored are kept.
    public String restoreDir = "restored";

    // Subdirectory for storing replication degrees
    public String chunksReplicationDegreeDir = "peersState/";

    // File to store the replication degree map's serializable
    public String chunksReplicationDegreePathName = chunksReplicationDegreeDir + "chunksReplicationDegree";

    // Multicaster's timeout for reading from multicast socket
    public int multicastTimeout = 300; // milliseconds

    // Peer socket's timeout for waiting on new queue message
    public int socketTimeout = 300; // milliseconds

    // Peer socket's message queue capacity, (in datagram packets)
    public int socketQueueCapacity = 10000;

    // Peer's thread pool size (in threads)
    public int threadPoolSize = 8;
    //public static int putchunkerPoolSize = 10;
}