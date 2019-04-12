package dbs;

import dbs.fileInfoManager.FileInfo;
import dbs.fileInfoManager.FileInfoManager;
import dbs.message.Message;
import dbs.message.MessageException;
import dbs.processor.ControlProcessor;
import dbs.processor.DataBackupProcessor;
import dbs.processor.DataRestoreProcessor;
import dbs.transmitter.DeleteTransmitter;
import dbs.transmitter.PutchunkTransmitter;
import dbs.transmitter.ReclaimHandler;
import dbs.transmitter.RestoreHandler;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Peer implements ClientInterface {

  private final static Logger LOGGER = Logger.getLogger(Peer.class.getName());
  // ^^^ Enforce use of Peer.log ?

  private static Peer peer;

  private Long id;
  private String accessPoint;

  private Multicaster mc;
  private Multicaster mdb;
  private Multicaster mdr;
  private PeerSocket socket;

  private ScheduledThreadPoolExecutor pool; // TODO: do not use scheduled

  public static Peer getInstance() {
    assert peer != null;
    return peer;
  }

  public static Peer createInstance(long id, String accessPoint) throws IOException {
    return peer == null ? (peer = new Peer(id, accessPoint)) : peer;
  }

  public static Peer createInstance(String accessPoint) throws IOException {
    return peer == null ? (peer = new Peer(accessPoint)) : peer;
  }

  public static Peer createInstance() throws IOException {
    return peer == null ? (peer = new Peer()) : peer;
  }

  public static void main(String[] args) {
    // Note: avoid using the Logger until we're alive
    // 1. Process args and create peer
    try {
      parseArgs(args);
    } catch (Exception e) {
      System.err.println("Could not create Peer instance.");
      e.printStackTrace();
      System.exit(1);
    }

    // 2. Initiate file managers
    try {
      FileInfoManager.createInstance();
    } catch (Exception e) {
      System.err.println("Could not create File Manager instance.");
      e.printStackTrace();
      System.exit(1);
    }

    // 3. Launch peer threads
    try {
      peer.init();
    } catch (Exception e) {
      System.err.println("Could not initiate peer instance.");
      e.printStackTrace();
      System.exit(1);
    }

    // 4. Assign the peer to the registry
    try {
      ClientInterface stub = (ClientInterface) UnicastRemoteObject.exportObject(peer, 0);
      // Bind the remote object's stub in the registry
      Registry registry = LocateRegistry.getRegistry();
      registry.rebind(peer.getAccessPoint(), stub);
      Peer.log("Ready to receive requests.\n", Level.INFO);
    } catch (Exception e) {
      System.err.println("Could not bind stub to the name " + peer.getAccessPoint());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void parseArgs(String[] args) throws Exception {
    if (args.length != 9) {
      System.err.println("Wrong number of arguments. Usage:\n" +
          "java Peer <protocol_version> <id> <access_point> <mc> <mdb> <mdr>");
      throw new IllegalArgumentException("Wrong number of arguments");
    }

    // parse protocol version
    Configuration.version = args[0];
    try {
      Message.validateVersion(args[0]);
    } catch (MessageException e) {
      System.err.println("Invalid protocol version: " + args[0]);
      throw e;
    }

    // parse id
    long id;
    try {
      id = Long.parseLong(args[1]);
    } catch (NumberFormatException e) {
      System.err.println("Wrong format for the second argument (peer id)");
      throw e;
    }

    // parse access point
    String accessPoint = args[2];

    // parse mc
    Protocol.mc = new MulticastChannel(args[3], args[4]);

    // parse mdb
    Protocol.mdb = new MulticastChannel(args[5], args[6]);

    // parse mdr
    Protocol.mdr = new MulticastChannel(args[7], args[8]);

    peer = new Peer(id, accessPoint);
  }

  private Peer(long id, @NotNull String accessPoint) throws IOException {
    this.id = id;
    this.accessPoint = accessPoint;

    setup();
  }

  private Peer(@NotNull String accessPoint) throws IOException {
    this.id = ThreadLocalRandom.current().nextLong();
    this.accessPoint = accessPoint;

    setup();
  }

  private Peer() throws IOException {
    this.id = ThreadLocalRandom.current().nextLong();
    this.accessPoint = getAccessPoint();

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

  private void initTransmitters() {
    RestoreHandler.createInstance();
  }

  private void initSocket() throws IOException {
    try {
      this.socket = new PeerSocket();
    } catch (Exception e) {
      LOGGER.severe("Could not create the peer's socket.\n");
      throw e;
    }
  }

  private void initMulticasters() throws IOException {
    this.mc = new Multicaster(Protocol.mc, new ControlProcessor());
    this.mdb = new Multicaster(Protocol.mdb, new DataBackupProcessor());
    this.mdr = new Multicaster(Protocol.mdr, new DataRestoreProcessor());
  }

  private void initPool() {
    this.pool = new ScheduledThreadPoolExecutor(Configuration.peerThreadPoolSize);
    // TODO: do not use scheduled, use a limited-size thread pool executor instead,
    // TODO: dropping received messages when we're overworked and can't handle them.
  }

  private void launchThreads() {
    // Launch a thread for each socket.
    Thread tSocket = new Thread(socket);
    Thread tMC = new Thread(mc);
    Thread tMDB = new Thread(mdb);
    Thread tMDR = new Thread(mdr);

    // Socket threads have higher priority.
    tSocket.setPriority(Thread.MAX_PRIORITY);
    tMC.setPriority(Thread.MAX_PRIORITY);
    tMDB.setPriority(Thread.MAX_PRIORITY);
    tMDR.setPriority(Thread.MAX_PRIORITY);

    tSocket.start();
    tMC.start();
    tMDB.start();
    tMDR.start();
  }

  /**
   * Construct all required sockets, joining the respective multicast channels; join the
   * thread pool, verify file paths and configuration, etc.
   *
   * @throws IOException
   */
  private void setup() throws IOException {
    initSocket();
    initMulticasters();
    initPool();
    initTransmitters();
  }

  private void init() {
    launchThreads();
  }

  /**
   * Outputs the given message according to the provided level.
   *
   * @param msg   The string message (or a key in the message catalog)
   * @param level One of the message level identifiers, e.g., SEVERE
   */
  public static void log(@NotNull String msg, Level level) {
    LOGGER.log(level, msg + ".\n");
  }

  public static void log(@NotNull String msg, Throwable e, Level level) {
    LOGGER.log(level, msg + ".\n" + e.getMessage());
  }

  public long getId() {
    return id;
  }

  public String getAccessPoint() {
    return accessPoint;
  }

  public ScheduledThreadPoolExecutor getPool() {
    return pool;
  }

  /********* Interface Implementation **********/
  public void backup(String pathname, int replicationDegree) {
    Peer.log("Received BACKUP request", Level.FINE);
    this.pool.submit(new PutchunkTransmitter(pathname, replicationDegree, 1));
  }

  public void restore(@NotNull String pathname) throws RemoteException {
    Peer.log("Received RESTORE request for " + pathname, Level.FINE);
    RestoreHandler.getInstance().initRestore(pathname);
  }

  public void delete(@NotNull String pathname, boolean runEnhancedVersion) throws RemoteException {
    Peer.log("Received DELETE request for " + pathname, Level.FINE);
    this.pool.submit(new DeleteTransmitter(pathname, 1, runEnhancedVersion));
  }

  public void reclaim(long maxDiskSpace) throws RemoteException {
    Peer.log("Received RECLAIM request with maximum " + maxDiskSpace, Level.FINE);
    ReclaimHandler.getInstance().initReclaim(maxDiskSpace);
  }

  public String state() throws RemoteException {
    Peer.log("Received STATE request", Level.FINE);
    return FileInfoManager.getInstance().dumpState();
  }
}
