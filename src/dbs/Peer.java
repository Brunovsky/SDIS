package dbs;

import dbs.message.Message;
import dbs.processor.ControlProcessor;
import dbs.processor.DataBackupProcessor;
import dbs.processor.DataRestoreProcessor;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;

public final class Peer {
  // NOTE: Package scope for testing purposes. Remove this later.
  Multicaster mc, mdb, mdr;
  PeerSocket socket;

  private final String id;
  private ScheduledThreadPoolExecutor pool;

  /**
   * Construct all Multicasters, the peer socket, and the thread pools.
   *
   * @throws IOException Might be generated by the Multicaster constructors if there
   *                     are problems with the protocol channels
   */
  private void init() throws IOException {
    ControlProcessor processorMC = new ControlProcessor();
    DataBackupProcessor processorMDB = new DataBackupProcessor();
    DataRestoreProcessor processorMDR = new DataRestoreProcessor();

    mc = new Multicaster(this, Protocol.MC, processorMC);
    mdb = new Multicaster(this, Protocol.MDB, processorMDB);
    mdr = new Multicaster(this, Protocol.MDR, processorMDR);

    pool = new ScheduledThreadPoolExecutor(Configuration.threadPoolSize);
  }

  /**
   * Launches the main threads: for the peer's socket and all multicasters.
   */
  void start() {
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

  /**
   * Construct a peer making no assumptions, generate a random peer id and initialize all
   * multicasters, the peer socket, and thread pools.
   */
  public Peer() throws IOException {
    this(Long.toString(ThreadLocalRandom.current().nextLong()));
  }

  /**
   * Construct a peer on a given port, generate a random peer id and initialize all
   * multicasters, the peer socket, and thread pools.
   *
   * @param port The peer socket's port
   */
  public Peer(int port) throws IOException {
    this(port, Long.toString(ThreadLocalRandom.current().nextLong()));
  }

  /**
   * Construct a peer on a given port and address, generate a random peer id and
   * initialize all multicasters, the peer socket, and thread pools.
   *
   * @param port    The peer socket's port
   * @param address The peer socket's address
   */
  public Peer(int port, @NotNull InetAddress address) throws IOException {
    this(port, address, Long.toString(ThreadLocalRandom.current().nextLong()));
  }

  /**
   * Construct a peer with a given peer id, and initialize all multicasters, the peer
   * socket, and thread pools.
   *
   * @param id The peer's id
   */
  public Peer(@NotNull String id) throws IOException {
    this.id = id;
    this.socket = new PeerSocket(this);
    init();
  }

  /**
   * Construct a peer with a given peer id on a given port, and initialize all
   * multicasters, the peer socket, and thread pools.
   *
   * @param port The peer socket's port
   * @param id   The peer's id
   */
  public Peer(int port, @NotNull String id) throws IOException {
    this.id = id;
    this.socket = new PeerSocket(this, port);
    init();
  }

  /**
   * Construct a peer with a given peer id on a given port and address, and initialize all
   * multicasters, the peer socket, and thread pools.
   *
   * @param port    The peer socket's port
   * @param address The peer socket's address
   * @param id      The peer's id
   */
  public Peer(int port, @NotNull InetAddress address, @NotNull String id)
      throws IOException {
    this.id = id;
    this.socket = new PeerSocket(this, port, address);
    init();
  }

  public final String getId() {
    return this.id;
  }

  public final ScheduledThreadPoolExecutor getPool() {
    return this.pool;
  }

  public final void send(@NotNull Message message, @NotNull Channel channel) {
    socket.send(message, channel);
  }
}
