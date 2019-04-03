package dbs;

import java.net.InetAddress;
import java.net.UnknownHostException;

public final class Protocol {
  public static String version = "1.0";

  public static int maxPacketSize = 65200;
  public static int chunkSize = 64000;

  public static Channel MC;
  public static Channel MDB;
  public static Channel MDR;

  // NOTE: For convenience, delete this later
  public static void setup() throws UnknownHostException {
    MC = new Channel(29500, InetAddress.getByName("237.0.0.1"));
    MDB = new Channel(29500, InetAddress.getByName("237.0.0.2"));
    MDR = new Channel(29500, InetAddress.getByName("237.0.0.3"));
  }
}
