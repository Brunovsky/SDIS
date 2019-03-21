package dbs;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ThreadLocalRandom;

public class Peer {
  public MulticastChannel control;
  public MulticastChannel mdb;
  public MulticastChannel mdr;
  public UdpChannel socket;

  protected String id;

  private void initMulticast() throws IOException {
    control = new MulticastChannel(this, Protocol.controlPort, Protocol.controlAddress);
    mdb = new MulticastChannel(this, Protocol.mdbPort, Protocol.mdbAddress);
    mdr = new MulticastChannel(this, Protocol.mdrPort, Protocol.mdrAddress);
  }

  /**
   * * Constructors
   * I don't know which ones will be needed right now. So I wrote all combinations :P
   */

  public Peer() throws IOException {
    this(ThreadLocalRandom.current().nextLong());
  }

  public Peer(int port) throws IOException {
    this(port, ThreadLocalRandom.current().nextLong());
  }

  public Peer(int port, InetAddress address) throws IOException {
    this(port, address, ThreadLocalRandom.current().nextLong());
  }

  public Peer(long id) throws IOException {
    this.id = Long.toString(id);
    this.socket = new UdpChannel(this);
    initMulticast();
  }

  public Peer(int port, long id) throws IOException {
    this.id = Long.toString(id);
    this.socket = new UdpChannel(this, port);
    initMulticast();
  }

  public Peer(int port, InetAddress address, long id) throws IOException {
    this.id = Long.toString(id);
    this.socket = new UdpChannel(this, port, address);
    initMulticast();
  }
}
