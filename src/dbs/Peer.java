package dbs;

import dbs.message.Message;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;


public class Peer implements ClientInterface {

  private String protocolVersion;
  private long id;
  private String accessPoint;
  private Multicaster mc;
  private Multicaster mdb;
  private Multicaster mdr;
  private PeerSocket socket;
  private ScheduledThreadPoolExecutor pool;
  private HashMap<ChunkKey,Integer> chunksReplicationDegree;
  File chunksReplicationDegreeFile;

  public static void main(String args[]) {

    Peer peer = null;

    // Process args
    try {
      peer = parseArgs(args);
    } catch (Exception e) {
      Utils.printErr("Peer", "Could not create Peer object.");
      System.exit(1);
    }

    try {
      ClientInterface stub = (ClientInterface) UnicastRemoteObject.exportObject(peer, 0);
      // Bind the remote object's stub in the registry
      Registry registry = LocateRegistry.getRegistry();
      registry.rebind(peer.getAccessPoint(), stub);
      Utils.printInfo("Peer", "Ready to receive requests.");
    } catch (Exception e) {
      e.printStackTrace();
      Utils.printErr("Peer", "Could not bind the remote object's stub to the name" + peer.getAccessPoint() + " in the registry.");
      System.exit(1);
    }
  }

  public static Peer parseArgs(String args[]) throws Exception {

    if (args.length != 9)
      Utils.printErr("Peer", "Wrong number of arguments. Usage: Peer <protocol_version> <id> <access_point> <mc> <mdb> <mdr>");

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
      Utils.printErr("Peer", "Invalid format for mc. Should be <ip_address><port_number>.");
      throw e;
    }

    // parse mdb
    MulticastChannel mdb;
    try {
      mdb = new MulticastChannel(args[5], args[6]);
    } catch (Exception e) {
      Utils.printErr("Peer", "Invalid format for mdb. Should be <ip_address><port_number>.");
      throw e;
    }

    // parse mdr
    MulticastChannel mdr;
    try {
      mdr = new MulticastChannel(args[7], args[8]);
    } catch (Exception e) {
      Utils.printErr("Peer", "Invalid format for mdr. Should be <ip_address><port_number>.");
      throw e;
    }

    return new Peer(protocolVersion, id, accessPoint, mc, mdb, mdr);
  }

  Peer(String protocolVersion, long id, String accessPoint, MulticastChannel mc, MulticastChannel mdb, MulticastChannel mdr) {
    this.protocolVersion = protocolVersion;
    this.id = id;
    this.accessPoint = accessPoint;
    try {
      this.mc = new Multicaster(this, mc, Multicaster.Processor.ProcessorType.CONTROL);
      this.mdb = new Multicaster(this, mdb, Multicaster.Processor.ProcessorType.DATA_BACKUP);
      this.mdr = new Multicaster(this, mdr, Multicaster.Processor.ProcessorType.DATA_RESTORE);
    } catch (Exception e) {
      Utils.printErr("Peer", "Could not create multicasters.");
      System.exit(1);
    }
    pool = new ScheduledThreadPoolExecutor(Configuration.threadPoolSize);
    try {
      this.socket = new PeerSocket(this, mc, mdb, mdr);
    } catch (Exception e) {
      Utils.printErr("Peer", "Could not create the peer's socket.");
      System.exit(1);
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
      Utils.printInfo("Peer", "Could not access the chunksReplicationDegree hashmap. Going to create one.");
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
      Utils.printInfo("Peer", "Could not locate the peer's state directory. Going to create one.");
    } catch (Exception e) {
      Utils.printErr("Peer", "Could not create the peersState directory.");
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
      Utils.printErr("Peer", "Could not generate serializes hashmap");
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
  public void backup(String pathname, int replicationDegree) throws RemoteException {
    File fileToBackup = new File(pathname);

    // check if path name corresponds to a valid file
    if (!fileToBackup.exists() || fileToBackup.isDirectory()) {
      Utils.printErr("Peer", "Invalid path name.");
      return;
    }

    // hash the pathname
    byte[] fileId;
    try {
      fileId = Utils.hash(fileToBackup, this.id);
      Utils.printInfo("Peer", "Received: " + fileId);
    } catch (Exception e) {
      Utils.printErr("Peer", "Could not retrieve a file id for the path name " + pathname);
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
