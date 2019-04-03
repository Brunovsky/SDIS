package dbs;

import org.jetbrains.annotations.NotNull;

import java.net.InetAddress;

public final class Channel {
  public final InetAddress address;
  public final int port;

  public Channel(int port, @NotNull InetAddress address) {
    assert port > 0;
    this.port = port;
    this.address = address;
  }
}
