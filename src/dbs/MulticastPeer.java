package dbs;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class MulticastPeer {
  public static final int timeout = 1000;
  public static final int multicastTimeout = 1000;

  private MulticastSocket multicastSocket;
  private InetAddress multicastGroup;
  private int multicastPort;

  private DatagramSocket udpSocket;
  private InetAddress udpAddress;
  private int udpPort;

  public MulticastPeer(int mPort, InetAddress mGroup) throws IOException {
    initMulticast(mPort, mGroup);

    udpSocket = new DatagramSocket();
    udpSocket.setSoTimeout(timeout);
    udpAddress = udpSocket.getLocalAddress();
    udpPort = udpSocket.getLocalPort();
  }

  public MulticastPeer(int mPort, InetAddress mGroup, int port) throws IOException {
    initMulticast(mPort, mGroup);

    udpSocket = new DatagramSocket(port);
    udpSocket.setSoTimeout(timeout);
    udpAddress = udpSocket.getLocalAddress();
    udpPort = udpSocket.getLocalPort();
  }

  public MulticastPeer(int mPort, InetAddress mGroup, int port, InetAddress address)
      throws IOException {
    initMulticast(mPort, mGroup);

    udpSocket = new DatagramSocket(port, address);
    udpSocket.setSoTimeout(timeout);
    udpAddress = udpSocket.getLocalAddress();
    udpPort = udpSocket.getLocalPort();
  }

  private void initMulticast(int mPort, InetAddress mGroup) throws IOException {
    this.multicastPort = mPort;
    this.multicastGroup = mGroup;

    multicastSocket = new MulticastSocket(multicastPort);
    multicastSocket.joinGroup(multicastGroup);
    multicastSocket.setSoTimeout(multicastTimeout);
    multicastSocket.setTimeToLive(1);
  }

  public void die() throws IOException {
    multicastSocket.leaveGroup(multicastGroup);
    multicastSocket.close();
    multicastSocket = null;

    udpSocket.close();
    udpSocket = null;
  }

  public void send(String message, InetAddress destAddress, int destPort)
      throws IOException {
    byte[] buffer = message.getBytes();
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length, destAddress,
                                               destPort);
    udpSocket.send(packet);
  }

  public DatagramPacket receive() throws IOException {
    byte[] buffer = new byte[4096];
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
    udpSocket.receive(packet);
    return packet;
  }

  public void sendMulticast(String message) throws IOException {
    send(message, multicastGroup, multicastPort);
  }

  public DatagramPacket receiveMulticast() throws IOException {
    byte[] buffer = new byte[4096];
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
    multicastSocket.receive(packet);
    return packet;
  }

  public int getPort() {
    return udpPort;
  }

  public InetAddress getAddress() {
    return udpAddress;
  }

  public int getMulticastPort() {
    return multicastPort;
  }

  public InetAddress getMulticastGroup() {
    return multicastGroup;
  }
}
