package dbs;

import java.net.*;

public class MulticastChannel {
  private final InetAddress address;
  private final int port;

  MulticastChannel(InetAddress address, int port) {
    this.address = address;
    this.port = port;
  }

  MulticastChannel(String address, String port) throws Exception {
    // parse address
    try {
      this.address = InetAddress.getByName(address);
    } catch (UnknownHostException e) {
      Utils.printErr("MulticastChannel", "Could not get an InetAddress object for the raw IP address " + address);
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
