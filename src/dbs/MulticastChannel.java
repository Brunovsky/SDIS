package dbs;

import java.net.*;
import java.util.logging.Logger;

public class MulticastChannel {
  private final InetAddress address;
  private final int port;
  private final static Logger LOGGER = Logger.getLogger(Peer.class.getName());

  MulticastChannel(InetAddress address, int port) {
    this.address = address;
    this.port = port;
  }

  MulticastChannel(String address, String port) throws Exception {
    // parse address
    try {
      this.address = InetAddress.getByName(address);
    } catch (UnknownHostException e) {
      LOGGER.severe("Could not get an InetAddress object for the raw IP address " + address + ".\n");
      throw e;
    }

    // parse port
    try {
      this.port = Integer.parseInt(port);
    } catch (NumberFormatException e) {
      throw e;
    }
  }

  public InetAddress getAddress() {
    return address;
  }

  public int getPort() {
    return port;
  }
}
