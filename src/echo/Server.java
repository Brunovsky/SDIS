package echo;

import java.io.IOException;
import java.net.*;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import dbs.MulticastPeer;

public class Server {
  private static MulticastPeer peer;
  private static String servername, echo;
  private static int timeout = 10000;  // 10 seconds

  private static void usage() {
    System.out.println("usage:");
    System.out.println("java echo.Server MULTI_PORT MULTI_ADDR FILENAME [PORT [ADDR]]");
    System.exit(0);
  }

  private static void init(String[] args) throws IOException {
    if (args.length < 3 || args.length > 5) usage();

    int mPort = Integer.parseInt(args[0]);
    InetAddress mGroup = InetAddress.getByName(args[1]);

    servername = args[2];
    echo = "[echo.Server " + servername + "] ";
    Files.createDirectories(Paths.get(servername));

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

    peer.setMulticastSoTimeout(timeout);
  }

  private static void handle(String message) throws IOException {
    try {
      Path path = Paths.get(servername, message);
      Files.createFile(path);
    } catch (FileAlreadyExistsException e) {
      System.out.print(echo + "File " + message + " already exists\n");
    } catch (IOException e) {
      System.out.print(echo + "File IO Exception: " + e.getMessage() + "\n");
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    init(args);

    long count = 0;
    System.out.printf(echo + "Listening on %s:%d / timeout %ds\n",
                      peer.getMulticastGroup(), peer.getMulticastPort(),
                      peer.getMulticastSoTimeout());

    try {
      while (true) {
        DatagramPacket packet = peer.receiveMulticast();
        String message = new String(packet.getData(), packet.getOffset(),
                                    packet.getLength());

        System.out.print(echo + "Received message " + message + "\n");
        ++count;
        handle(message);

        peer.send(servername, packet.getAddress(), packet.getPort());
      }
    } catch (SocketTimeoutException e) {
      System.out.print(echo + "Timeout. Received " + count + " total messages\n");
    } catch (IOException e) {
      System.out.print(echo + "Socket IO Exception: " + e.getMessage() + "\n");
      e.printStackTrace();
    }

    peer.die();
  }
}
