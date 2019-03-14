package echo;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;

public class Client {
  private static MulticastSocket multicast;
  private static InetAddress group;
  private static int mport;

  private static DatagramSocket socket;

  private static String filename, echo;

  private static final int timeout = 1000;

  private static void usage() {
    System.out.println("usage:");
    System.out.println("ECHO CLIENT :: java EchoClient MULTI_PORT MULTICAST FILENAME [SOCKET_PORT [ADDRESS]]");
    System.exit(0);
  }

  private static void init(String[] args) throws IOException {
    if (args.length < 3 || args.length > 5)
      usage();

    mport = Integer.parseInt(args[0]);
    group = InetAddress.getByName(args[1]);

    multicast = new MulticastSocket(mport);
    multicast.joinGroup(group);
    multicast.setSoTimeout(timeout);
    multicast.setTimeToLive(1);

    filename = args[2];
    echo = "[EchoClient " + filename + "] ";

    if (args.length == 3) {
      socket = new DatagramSocket();
    } else if (args.length == 4) {
      socket = new DatagramSocket(Integer.parseInt(args[3]));
    } else {
      socket = new DatagramSocket(Integer.parseInt(args[3]), InetAddress.getByName(args[4]));
    }

    socket.setSoTimeout(timeout);
  }

  private static void die() throws IOException {
    multicast.leaveGroup(group);
    multicast.close();
    socket.close();
  }

  private static void sendSocket(String message, InetAddress destAddress, int destPort) throws IOException {
    byte[] buffer = message.getBytes();
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length, destAddress, destPort);
    socket.send(packet);
  }

  private static DatagramPacket receiveSocket() throws IOException {
    byte[] buffer = new byte[4096];
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
    socket.receive(packet);
    return packet;
  }

  private static void sendMulticast(String message) throws IOException {
    sendSocket(message, group, mport);
  }

  private static DatagramPacket receiveMulticast() throws IOException {
    byte[] buffer = new byte[4096];
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
    multicast.receive(packet);
    return packet;
  }

  public static void main(String[] args) throws IOException {
    init(args);

    long count = 0;
    System.out.printf(echo + "Sending ECHO %s on %s:%d / timeout %ds\n", filename, group, mport, timeout / 1000);

    sendMulticast(filename);

    // Receive responses
    try {
      while (true) {
        DatagramPacket packet = receiveSocket();
        String message = new String(packet.getData(), packet.getOffset(), packet.getLength());

        System.out.println(echo + "Received echo " + message);
        ++count;
      }
    } catch (SocketTimeoutException e) {
      System.out.println(echo + "Timeout. Received " + count + " total echoes");
    } catch (IOException e) {
      System.out.println(echo + "Socket IO Exception: " + e.getMessage());
      e.printStackTrace();
    }

    die();
  }
}
