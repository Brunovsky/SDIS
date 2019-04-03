package dbs;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class Peer implements ClientInterface {


  private String protocolVersion;
  private long id;
  private String accessPoint;
  private Multicaster mc;
  private Multicaster mdb;
  private Multicaster mdr;
  private ScheduledThreadPoolExecutor pool;

  public static void main(String args[]) {

    Peer peer = null;

    // Process args
    try {
      peer = parseArgs(args);
    } catch(Exception e) {
      Utils.printErr("Peer","Could not create Peer object.");
      System.exit(1);
    }

    try {
        ClientInterface stub = (ClientInterface) UnicastRemoteObject.exportObject(peer, 0 );
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

    if(args.length != 9)
      Utils.printErr("Peer", "Wrong number of arguments. Usage: Peer <protocol_version> <id> <access_point> <mc> <mdb> <mdr>");

    // parse protocol version
    String protocolVersion = args[0];

    // parse id
    long id;
    try {
      id = Long.parseLong(args[1]);
    } catch(NumberFormatException e) {
      System.err.println("Wrong format for the second argument (server_id)");
      throw e;
    }

    // parse access point
    String accessPoint = args[2];

    // parse mc
    MulticastChannel mc;
    try {
      mc = new MulticastChannel(args[3], args[4]);
    } catch(Exception e) {
      Utils.printErr("Peer", "Invalid format for mc. Should be <ip_address><port_number>.");
      throw e;
    }

    // parse mdb
    MulticastChannel mdb;
    try {
      mdb = new MulticastChannel(args[5], args[6]);
    } catch(Exception e) {
        Utils.printErr("Peer", "Invalid format for mdb. Should be <ip_address><port_number>.");
        throw e;
    }

    // parse mdr
    MulticastChannel mdr;
    try {
      mdr = new MulticastChannel(args[7], args[8]);
    }
    catch (Exception e) {
        Utils.printErr("Peer", "Invalid format for mdr. Should be <ip_address><port_number>.");
        throw e;
    }

    return new Peer(protocolVersion, id, accessPoint, mc, mdb, mdr);
  }

  Peer(String protocolVersion, long id, String accessPoint, MulticastChannel mc, MulticastChannel mdb, MulticastChannel mdr) {
    this.protocolVersion = protocolVersion;
    this.accessPoint = accessPoint;
    try {
      this.mc = new Multicaster(this, mc, Multicaster.Processor.ProcessorType.CONTROL);
      this.mdb = new Multicaster(this, mdb, Multicaster.Processor.ProcessorType.DATA_BACKUP);
      this.mdr = new Multicaster(this, mdr, Multicaster.Processor.ProcessorType.DATA_RESTORE);
    } catch(Exception e) {
      Utils.printErr("Peer", "Could not create multicasters.");
      System.exit(1);
    }
    pool = new ScheduledThreadPoolExecutor(Configuration.threadPoolSize);
    this.start();
  }

  private void start() {
    //Thread tSocket = new Thread(socket);
    Thread tMC = new Thread(mc);
    Thread tMDB = new Thread(mdb);
    Thread tMDR = new Thread(mdr);

    //tSocket.setPriority(Thread.MAX_PRIORITY);
    tMC.setPriority(Thread.MAX_PRIORITY);
    tMDB.setPriority(Thread.MAX_PRIORITY);
    tMDR.setPriority(Thread.MAX_PRIORITY);

    //tSocket.start();
    tMC.start();
    tMDB.start();
    tMDR.start();
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
  public void backup(String pathname, int replicationDegree) throws RemoteException  {
    return;
  }
  
  public void restore(String pathname) throws RemoteException {
    return;
  }
  
  public void delete(String pathname) throws RemoteException {
    return;
  }
  
  public void reclaim(int maxDiskSpaceChuncks) throws RemoteException {
    return;
  }
  
  public String state() throws RemoteException {
    return "Hi there!";
  }
}
