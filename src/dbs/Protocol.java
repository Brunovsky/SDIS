package dbs;

import java.net.InetAddress;

public final class Protocol {
  public static String version = "1.0";

  public static int multicastTimeout = 2000;
  public static int udpTimeout = 3000;

  public static final int maxBodySize = 64000;
  public static final int maxPacketSize = 65200;

  public static InetAddress controlAddress;
  public static int controlPort;

  public static InetAddress mdbAddress;
  public static int mdbPort;

  public static InetAddress mdrAddress;
  public static int mdrPort;
}
