package dbs;

public final class Protocol {
  public static final int chunkSize = 64000;
  public static final int maxPacketSize = 65200;

  // Where to put this?
  public static int registryPort = 29001;

  // minimum delay time (for the schedule of new threads) - ms
  public static int minDelay = 0;

  // maximum delay time (for the schedule of new threads) - ms
  public static int maxDelay = 400;

  // time interval for a putchunker to collect the STORED messages, on a first attempt - s
  public static int delayReceiveStored = 1;

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
