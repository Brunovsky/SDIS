package dbs;

public final class Protocol {
  public static final int maxBodySize = 64000;
  public static final int maxPacketSize = 65200;

  public static int registryPort = 29001;

  public static MulticastChannel mc;
  public static MulticastChannel mdb;
  public static MulticastChannel mdr;
}
