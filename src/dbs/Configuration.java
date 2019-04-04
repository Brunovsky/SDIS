package dbs;

public class Configuration {
    public static String chunksReplicationDegreeDir = "peersState/";
    public static String chunksReplicationDegreePathName = chunksReplicationDegreeDir + "chunksReplicationDegree";
    public static int maxPacketSize = 65200;
    public static int chunkSize = 64000;
    public static int multicastTimeout = 300; // milliseconds
    public static int socketTimeout = 300; // milliseconds
    public static int socketQueueCapacity = 10000;
    public static int threadPoolSize = 8;
    //public static int putchunkerPoolSize = 10;
}