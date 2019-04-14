package dbs;

public final class Protocol {

  public static final int chunkSize = 64000;
  public static final int maxPacketSize = 65200;

  // minimum delay time (for the schedule of new threads) - ms
  public static int minDelay = 0;

  // maximum delay time (for the schedule of new threads) - ms
  public static int maxDelay = 2000;

  // time interval for a putchunker to wait for a CHUNK message before retrying
  public static int delayPutchunker = 2500;

  // time interval for a getchunker to wait for a CHUNK message before retrying
  public static int delayGetchunker = 2500;

  // number of times the DELETE message is sent
  public static int numberDeleteMessages = 5;

  // time interval between the transmission of DELETE messages - s
  public static int delaySendDelete = 1;

  // new message's version
  public static String newMessagesVersion = "1.1";

  public static MulticastChannel mc;
  public static MulticastChannel mdb;
  public static MulticastChannel mdr;
}
