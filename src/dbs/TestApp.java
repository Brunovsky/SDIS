package dbs;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.logging.Logger;

/**
 * A class to represent the testing client application.
 */
public class TestApp {
  /**
   * Represents the possible operations requested by the client.
   */
  private enum Operation {
    BACKUP, RESTORE, DELETE, RECLAIM, STATE
  }

  /**
   * The peer's access point in the RMI protocol.
   */
  private String peerAccessPoint;
  /**
   * The operation requested by the client.
   */
  private Operation operation;
  /**
   * True if the operation should be performed using the enhanced version of the
   * operation and false otherwise.
   */
  private boolean enh = false;
  /**
   * Either the path name of the file to backup/restore/delete or, in the case of
   * RECLAIM the maximum amount of disk space (in KByte) that the service can use
   * to store the chunks.
   */
  private String oper1;
  /**
   * This operand is an integer that specifies the desired replication degree and
   * applies only to the backup protocol (or its enhancement).
   */
  private int oper2;
  /**
   * Used to log important messages that can be read by the user.
   */
  private final static Logger LOGGER = Logger.getLogger(TestApp.class.getName());

  public static void main(String[] args) {
    if (!(args.length >= 2 && args.length <= 4)) {
      System.out.println("    Wrong number of arguments. Usage:");
      System.out.println("        java TestApp <peer_ap> <sub_protocol> <opnd_1> <opnd_2>");
      System.out.println("        <sub_protocol> should be one of: BACKUP, RESTORE, DELETE, RECLAIM, STATE");
      System.out.println("        The string ENH can be appended to the end of the subprotocol name.");
      System.out.println(
              "        In the case of the BACKUP, RESTORE and DELETE subprotocols, <opnd_1> should be a path name");
      System.out.println(
              "        In the case of the RECLAIM subprotocols, <opnd_1> should be the maximum ammount of disk space (KByte).");
      System.out.println(
              "        In the case of the RECLAIM subprotocols, <opnd_1> should be the maximum ammount of disk space (KByte).");
      System.exit(1);
    }

    TestApp testApp = new TestApp(args);
    testApp.run();
  }

  /**
   * Initializes the TestApp class attributes.
   *
   * @param args The arguments provided in the terminal.
   */
  public TestApp(String[] args) {
    this.peerAccessPoint = args[0];
    this.parseOperation(args[1]);

    if (this.operation == Operation.BACKUP || this.operation == Operation.RESTORE
            || this.operation == Operation.DELETE) {
      this.oper1 = args[2]; // pathname
      if (this.operation == Operation.BACKUP) {
        if (args.length >= 4) {
          try {
            this.oper2 = Integer.parseInt(args[3]);
          } catch (NumberFormatException e) {
            LOGGER.severe("The second operand " + args[3] + " is not allowed. " + "Should be an integer for the BACKUP operation\n");
            System.exit(1);
          }
        }
      }
    } else if (this.operation == Operation.RECLAIM) {
      try {
        Integer.parseInt(args[2]);
        this.oper1 = args[2];
      } catch (NumberFormatException e) {
        LOGGER.severe("The first operand " + args[2] + " is not allowed. " + "Should be an integer for the RECLAIM operation.\n");
        System.exit(1);
      }
    }
  }

  /**
   * Initializes the attributed enh and operation, based on the operation
   * specified in the commnad line.
   *
   * @param operation The operation to be performed by the peer (the string can
   *                  either end in ENH - to request the enhanced version of the
   *                  protocol - or not.
   */
  private void parseOperation(String operation) {
    if (operation.matches(".*ENH")) {
      this.enh = true;
      operation = operation.substring(0, operation.length() - 3);
    }

    switch (operation) {
      case "BACKUP":
        this.operation = Operation.BACKUP;
        break;
      case "RESTORE":
        this.operation = Operation.RESTORE;
        break;
      case "DELETE":
        this.operation = Operation.DELETE;
        break;
      case "RECLAIM":
        this.operation = Operation.RECLAIM;
        break;
      case "STATE":
        this.operation = Operation.STATE;
        break;
      default:
        LOGGER.severe("Operation " + operation + " is not allowed. Should be one of : BACKUP, RESTORE, DELETE, RECLAIM, STATE.\n");
        System.exit(1);
    }
  }

  private void logInfo() {

    String msg = "Successfully request a ";

    switch (this.operation) {
      case BACKUP:
        msg += " backup of the file " + this.oper1;
        break;
      case RESTORE:
        msg += " restoration of the file " + this.oper1;
        break;
      case DELETE:
        msg += " deletion of the file " + this.oper1;
        break;
      case RECLAIM:
        msg += " reclamation of " + this.oper1 + " KB of disk space.";
        break;
    }
    LOGGER.info(msg);
  }

  private void run() {
    try {
      Registry registry = LocateRegistry.getRegistry(); // the local host address should be used
      ClientInterface stub = (ClientInterface) registry.lookup(this.peerAccessPoint);

      switch (this.operation) {
        case BACKUP:
          stub.backup(this.oper1, this.oper2);
          break;
        case RESTORE:
          stub.restore(this.oper1);
          break;
        case DELETE:
          stub.restore(this.oper1);
          break;
        case RECLAIM:
          stub.reclaim(Integer.parseInt(this.oper1));
          break;
        case STATE:
          String state =  stub.state();
          return;
        default:
          break;
      }
      this.logInfo();

    } catch (Exception e) {
      LOGGER.warning("Could not invoke the remote object's method. Peer not available.\n");
    }
  }
}