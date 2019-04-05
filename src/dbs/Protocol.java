package dbs;

public final class Protocol {

  public static int multicastTimeout = 2000;
  public static int udpTimeout = 3000;

  public static final int maxBodySize = 64000;
  public static final int maxPacketSize = 65200;

  public static String mcAddress = "224.0.0.1";
  public static String mcPort = "8081";

  public static String mdbAddress = "224.0.0.2";
  public static String mdbPort = "8082";

  public static String mdrAddress = "224.0.0.3";
  public static String mdrPort = "8083";
}
