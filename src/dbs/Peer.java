package dbs;

import dbs.message.Message;
import dbs.processor.ControlProcessor;
import dbs.processor.DataBackupProcessor;
import dbs.processor.DataRestoreProcessor;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Logger;

public class Peer implements ClientInterface {
  private final static Logger LOGGER = Logger.getLogger(Peer.class.getName());

  private long id;
  private String accessPoint;
  private Configuration config;
  private Multicaster mc;
  private Multicaster mdb;
  private Multicaster mdr;
  private PeerSocket socket;
  private ScheduledThreadPoolExecutor pool;
  private HashMap<ChunkKey,Integer> chunksReplicationDegree;
  private File chunksReplicationDegreeFile;

  public static void main(String[] args) {

    Peer peer = null;

    // Process args
    try {
      peer = parseArgs(args);
    } catch (Exception e) {
      LOGGER.severe("Could not create Peer object.\n");
      System.exit(1);
    }

    try {
      ClientInterface stub = (ClientInterface) UnicastRemoteObject.exportObject(peer, 0);
      // Bind the remote object's stub in the registry
      Registry registry = LocateRegistry.getRegistry();
      registry.bind(peer.getAccessPoint(), stub);
      LOGGER.info("Ready to receive requests.\n");
    } catch (Exception e) {
      LOGGER.severe("Could not bind the remote object's stub to the name " + peer.getAccessPoint() + " in the registry.\n");
      System.exit(1);
    }
  }

  public static Peer parseArgs(String[] args) throws Exception {

    if (args.length != 9)
      LOGGER.warning("Wrong number of arguments. Usage: Peer <protocol_version> <id> <access_point> <mc> <mdb> <mdr>\n");

    // parse protocol version
    String protocolVersion = args[0];

    // parse id
    long id;
    try {
      id = Long.parseLong(args[1]);
    } catch (NumberFormatException e) {
      System.err.println("Wrong format for the second argument (server_id)");
      throw e;
    }

    // parse access point
    String accessPoint = args[2];

    // parse mc
    MulticastChannel mc;
    try {
      mc = new MulticastChannel(args[3], args[4]);
    } catch (Exception e) {
      LOGGER.warning("Invalid format for mc. Should be <ip_address><port_number>.\n");
      throw e;
    }

    // parse mdb
    MulticastChannel mdb;
    try {
      mdb = new MulticastChannel(args[5], args[6]);
    } catch (Exception e) {
      LOGGER.warning("Invalid format for mdb. Should be <ip_address><port_number>.\n");
      throw e;
    }

    // parse mdr
    MulticastChannel mdr;
    try {
      mdr = new MulticastChannel(args[7], args[8]);
    } catch (Exception e) {
      LOGGER.warning("Invalid format for mdr. Should be <ip_address><port_number>.\n");
      throw e;
    }

    return new Peer(protocolVersion, id, accessPoint, mc, mdb, mdr);
  }

  Peer(String protocolVersion, long id, String accessPoint, MulticastChannel mc,
       MulticastChannel mdb, MulticastChannel mdr) throws IOException {
    Protocol.mc = mc;
    Protocol.mdb = mdb;
    Protocol.mdr = mdr;

    this.id = id;
    this.accessPoint = accessPoint;
    this.config = new Configuration();
    this.config.version = protocolVersion;

    setup();
  }

  Peer(String protocolVersion, long id, String accessPoint) throws IOException {
    this.id = id;
    this.accessPoint = accessPoint;
    this.config = new Configuration();
    this.config.version = protocolVersion;

    setup();
  }

  Peer(long id, String accessPoint) throws IOException {
    this.id = id;
    this.accessPoint = accessPoint;
    this.config = new Configuration();

    setup();
  }

  Peer(long id, String accessPoint, Configuration config) throws IOException {
    this.id = id;
    this.accessPoint = accessPoint;
    this.config = config;

    setup();
  }

  /**
   * Orders all main threads to terminate orderly.
   * These threads will end within one socket reading cycle.
   */
  void finish() {
    socket.finish();
    mc.finish();
    mdb.finish();
    mdr.finish();
  }

  public void send(@NotNull Message message) {
    this.socket.send(message);
  }

  private void createChunksReplicationDegreeFile() {
    try {
      new File(config.chunksReplicationDegreeDir).mkdir();
      LOGGER.info("Could not locate the peer's state directory. Going to create one.\n");
    } catch (Exception e) {
      LOGGER.severe("Could not create the peersState directory.\n");
      System.exit(1);
    }
    String chunksReplicationDegreePathName =
        config.chunksReplicationDegreePathName + id + ".ser";
    this.chunksReplicationDegreeFile = new File(chunksReplicationDegreePathName);
    this.updateChunksReplicationDegreeHashMap();
  }

  private void updateChunksReplicationDegreeHashMap() {
    if (chunksReplicationDegreeFile.exists())
      this.chunksReplicationDegreeFile.delete();
    try {
      String chunksReplicationDegreePathName =
          config.chunksReplicationDegreePathName + id + ".ser";
      FileOutputStream fos = new FileOutputStream(chunksReplicationDegreePathName);
      ObjectOutputStream oos = new ObjectOutputStream(fos);
      oos.writeObject(this.chunksReplicationDegree);
      oos.close();
      fos.close();
    } catch (Exception e) {
      LOGGER.severe("Could not generate serializes hashmap.\n");
      System.exit(1);
    }
  }

  private void insertIntoChunksReplicationDegreeHashMap(byte[] fileId, int chunkNumber, int replicationDegree) {
    ChunkKey chunkKey = new ChunkKey(fileId, chunkNumber);
    this.chunksReplicationDegree.put(chunkKey, replicationDegree);
    this.updateChunksReplicationDegreeHashMap();
  }

  private void deleteFromChunksReplicationDegreeHashMap(byte[] fileId, int chunkNumber) {
    ChunkKey chunkKey = new ChunkKey(fileId, chunkNumber);
    this.chunksReplicationDegree.remove(chunkKey);
    this.updateChunksReplicationDegreeHashMap();
  }

  private void initMulticasters() throws IOException {
    try {
      this.mc = new Multicaster(this, Protocol.mc, new ControlProcessor());
      this.mdb = new Multicaster(this, Protocol.mdb, new DataBackupProcessor());
      this.mdr = new Multicaster(this, Protocol.mdr, new DataRestoreProcessor());
    } catch (IOException e) {
      LOGGER.severe("Could not create multicasters.\n");
      throw e;
    }
  }

  private void initSocket() throws IOException {
    try {
      this.socket = new PeerSocket(this);
    } catch (Exception e) {
      LOGGER.severe("Could not create the peer's socket.\n");
      throw e;
    }
  }

  private void initPool() {
    this.pool = new ScheduledThreadPoolExecutor(config.threadPoolSize);
  }

  private void launchThreads() {
    // Launch a thread for each socket.
    Thread tSocket = new Thread(socket);
    Thread tMC = new Thread(mc);
    Thread tMDB = new Thread(mdb);
    Thread tMDR = new Thread(mdr);

    tSocket.setPriority(Thread.MAX_PRIORITY);
    tMC.setPriority(Thread.MAX_PRIORITY);
    tMDB.setPriority(Thread.MAX_PRIORITY);
    tMDR.setPriority(Thread.MAX_PRIORITY);

    tSocket.start();
    tMC.start();
    tMDB.start();
    tMDR.start();
  }

  private void readReplicationDegreeMap() {
    // retrieve chunk's replication degree HashMap
    try {
      String chunksReplicationDegreePathName =
          config.chunksReplicationDegreePathName + id + ".ser";
      FileInputStream fis = new FileInputStream(chunksReplicationDegreePathName);
      ObjectInputStream ois = new ObjectInputStream(fis);
      this.chunksReplicationDegree = (HashMap) ois.readObject();
      fis.close();
      ois.close();
      this.chunksReplicationDegreeFile = new File(chunksReplicationDegreePathName);
      this.updateChunksReplicationDegreeHashMap();
    } catch (Exception e) {
      LOGGER.info("Could not access the chunksReplicationDegree hashmap. Going to create one.\n");
      this.chunksReplicationDegree = new HashMap<>();
      this.createChunksReplicationDegreeFile();
    }
  }

  /**
   * Construct all required sockets, joining the respective multicast channels; join the
   * thread pool, verify file paths and configuration, etc.
   * @throws IOException
   */
  private void setup() throws IOException {
    initSocket();
    initPool();
    initMulticasters();
    readReplicationDegreeMap();
    launchThreads();
  }

  public long getId() {
    return id;
  }

  public String getAccessPoint() {
    return accessPoint;
  }

  public Configuration getConfig() {
    return config;
  }

  public ScheduledThreadPoolExecutor getPool() {
    return pool;
  }

  /********* Interface Implementation **********/
  public void backup(String pathname, int replicationDegree) throws RemoteException {
    File fileToBackup = new File(pathname);

    // check if path name corresponds to a valid file
    if (!fileToBackup.exists() || fileToBackup.isDirectory()) {
      LOGGER.severe("Invalid path name.\n");
      return;
    }

    // hash the pathname
    byte[] fileId;
    try {
      fileId = Utils.hash(fileToBackup, this.id);
    } catch (Exception e) {
      LOGGER.severe("Could not retrieve a file id for the path name " + pathname + "\n");
      return;
    }

    // send PUTCHUNCK MESSAGES
    // TODO: continue - split file into chunks. Call method send of the Peer class for each message created.
    /*FileInputStream fis = new FileInputStream(fileToBackup);
    byte[] chunk = new byte[Configuration.chunkSize];
    while(fis.read(chunk, 0, Configuration.chunkSize)) {
    }
    */
  }

  public void restore(String pathname) throws RemoteException {
    return;
  }

  public void delete(String pathname) throws RemoteException {
    return;
  }

  public void reclaim(int maxDiskSpaceChunks) throws RemoteException {
    return;
  }

  public String state() throws RemoteException {
    return "Hi there!";
  }
}
