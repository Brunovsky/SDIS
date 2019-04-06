package dbs;

import dbs.message.Message;
import dbs.transmitter.DataBackupTransmitter;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Logger;

public class Peer implements ClientInterface {

  private String protocolVersion;
  private Long id;
  private String accessPoint;
  private Multicaster mc;
  private Multicaster mdb;
  private Multicaster mdr;
  private PeerSocket socket;
  private ScheduledThreadPoolExecutor pool;
  public HashMap<ChunkKey, Vector<Long>> chunksReplicationDegree;
  private File chunksReplicationDegreeFile;
  public final static Logger LOGGER = Logger.getLogger(Peer.class.getName());

  public static void main(String args[]) {

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
      registry.rebind(peer.getAccessPoint(), stub);
      LOGGER.info("Ready to receive requests.\n");
    } catch (Exception e) {
      LOGGER.severe("Could not bind the remote object's stub to the name " + peer.getAccessPoint() + " in the registry.\n");
      System.exit(1);
    }
  }

  public static Peer parseArgs(String args[]) throws Exception {

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

  Peer(String protocolVersion, long id, String accessPoint, MulticastChannel mc, MulticastChannel mdb, MulticastChannel mdr) throws Exception {
    this.protocolVersion = protocolVersion;
    this.id = id;
    this.accessPoint = accessPoint;
    try {
      this.mc = new Multicaster(this, mc, Multicaster.Processor.ProcessorType.CONTROL);
      this.mdb = new Multicaster(this, mdb, Multicaster.Processor.ProcessorType.DATA_BACKUP);
      this.mdr = new Multicaster(this, mdr, Multicaster.Processor.ProcessorType.DATA_RESTORE);
    } catch (Exception e) {
      LOGGER.severe("Could not create multicasters.\n");
      throw e;
    }
    this.pool = new ScheduledThreadPoolExecutor(Configuration.threadPoolSize);
    try {
      this.socket = new PeerSocket(this, mc, mdb, mdr);
    } catch (Exception e) {
      LOGGER.severe("Could not create the peer's socket.\n");
      throw e;
    }
    this.start();
  }

  Peer(String protocolVersion, long id, String accessPoint) throws Exception {
    this.protocolVersion = protocolVersion;
    this.id = id;
    this.accessPoint = accessPoint;
    MulticastChannel mcChannel = null;
    MulticastChannel mdbChannel = null;
    MulticastChannel mdrChannel = null;

    try {
      mcChannel = new MulticastChannel(Protocol.mcAddress, Protocol.mcPort);
      mdbChannel = new MulticastChannel(Protocol.mdbAddress, Protocol.mdbPort);
      mdrChannel = new MulticastChannel(Protocol.mdrAddress, Protocol.mdrPort);

      this.mc = new Multicaster(this, mcChannel, Multicaster.Processor.ProcessorType.CONTROL);
      this.mdb = new Multicaster(this, mdbChannel, Multicaster.Processor.ProcessorType.DATA_BACKUP);
      this.mdr = new Multicaster(this, mdrChannel, Multicaster.Processor.ProcessorType.DATA_RESTORE);
    } catch (Exception e) {
      LOGGER.severe("Could not create multicasters.\n");
      throw e;
    }
    this.pool = new ScheduledThreadPoolExecutor(Configuration.threadPoolSize);
    try {
      this.socket = new PeerSocket(this, mcChannel, mdbChannel, mdrChannel);
    } catch (Exception e) {
      LOGGER.severe("Could not create the peer's socket.\n");
      throw e;
    }
    this.start();
  }

  private void start() {
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

    // retrieve chunk's replication degree HashMap
    try {
      String chunksReplicationDegreePathName = Utils.getChunksReplicationDegreePathName(this.id);
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
   * Orders all main threads to terminate orderly.
   * These threads will end within one socket reading cycle.
   */
  void finish() {
    socket.finish();
    mc.finish();
    mdb.finish();
    mdr.finish();
  }

  public final void send(@NotNull Message message) {
    this.socket.send(message);
  }

  private void createChunksReplicationDegreeFile() {
    try {
      new File(Configuration.chunksReplicationDegreeDir).mkdir();
      LOGGER.info("Could not locate the peer's state directory. Going to create one.\n");
    } catch (Exception e) {
      LOGGER.severe("Could not create the peersState directory.\n");
      System.exit(1);
    }
    String chunksReplicationDegreePathName = Utils.getChunksReplicationDegreePathName(this.id);
    this.chunksReplicationDegreeFile = new File(chunksReplicationDegreePathName);
    this.updateChunksReplicationDegreeHashMap();
  }

  private void updateChunksReplicationDegreeHashMap() {
    if (chunksReplicationDegreeFile.exists())
      this.chunksReplicationDegreeFile.delete();
    try {
      String chunksReplicationDegreePathName = Utils.getChunksReplicationDegreePathName(this.id);
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

  private void insertIntoChunksReplicationDegreeHashMap(byte[] fileId, int chunkNumber, Long peerId) {
    ChunkKey chunkKey = new ChunkKey(fileId, chunkNumber);
    Vector<Long> chunKPeers = this.chunksReplicationDegree.get(chunkKey);
    if(chunKPeers == null)
      chunKPeers = new Vector<>();
    chunKPeers.add(peerId);
    //this.chunksReplicationDegree.put(chunkKey, chunKPeers);
    this.updateChunksReplicationDegreeHashMap();
  }

  private void deleteFromChunksReplicationDegreeHashMap(byte[] fileId, int chunkNumber) {
    ChunkKey chunkKey = new ChunkKey(fileId, chunkNumber);
    this.chunksReplicationDegree.remove(chunkKey);
    this.updateChunksReplicationDegreeHashMap();
  }

  public String getProtocolVersion() {
    return protocolVersion;
  }

  public long getId() {
    return id;
  }

  public String getAccessPoint() {
    return accessPoint;
  }

  public Multicaster getMc() {
    return mc;
  }

  public Multicaster getMdb() {
    return mdb;
  }

  public Multicaster getMdr() {
    return mdr;
  }

  public ScheduledThreadPoolExecutor getPool() {
    return pool;
  }

  /********* Interface Implementation **********/
  public void backup(String pathname, int replicationDegree) {
    this.LOGGER.info("Received BACKUP request.");
    this.pool.submit(new DataBackupTransmitter(this, pathname, replicationDegree));
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
