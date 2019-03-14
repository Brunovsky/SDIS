package echo;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;

import dbs.MulticastPeer;

public class Client {
  private static MulticastPeer peer;

  private static String filename, echo;

  private static void usage() {
    System.out.println("usage:");
    System.out.println("java echo.Client MULTI_PORT MULTI_ADDR FILENAME [PORT [ADDR]]");
    System.exit(0);
  }

  private static void init(String[] args) throws IOException {
    if (args.length < 3 || args.length > 5) usage();

    int mPort = Integer.parseInt(args[0]);
    InetAddress mGroup = InetAddress.getByName(args[1]);

    filename = args[2];
    echo = "[echo.Client " + filename + "] ";

    if (args.length == 3) {
      peer = new MulticastPeer(mPort, mGroup);
    } else if (args.length == 4) {
      int port = Integer.parseInt(args[3]);
      peer = new MulticastPeer(mPort, mGroup, port);
    } else {
      int port = Integer.parseInt(args[3]);
      InetAddress address = InetAddress.getByName(args[4]);
      peer = new MulticastPeer(mPort, mGroup, port, address);
    }
  }

  public static void main(String[] args) throws IOException {
    init(args);

    long count = 0;
    System.out.printf(echo + "Run %s on %s:%d / timeout %ds\n", filename,
                      peer.getAddress(), peer.getPort(), MulticastPeer.timeout / 1000);

    peer.sendMulticast(filename);

    // Receive responses
    try {
      while (true) {
        DatagramPacket packet = peer.receive();
        String message = new String(packet.getData(), packet.getOffset(),
                                    packet.getLength());

        System.out.println(echo + "Received echo " + message);
        ++count;
      }
    } catch (SocketTimeoutException e) {
      System.out.println(echo + "Timeout. Received " + count + " total echoes");
    } catch (IOException e) {
      System.out.println(echo + "Socket IO Exception: " + e.getMessage());
      e.printStackTrace();
    }

    peer.die();
  }
}
