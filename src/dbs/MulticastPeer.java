package dbs;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class MulticastPeer {
  public static final int timeout = 1000;

  private MulticastSocket multicastSocket;
  private InetAddress multicastGroup;
  private int multicastPort;

  private DatagramSocket peerSocket;
  private InetAddress peerAddress;
  private int peerPort;

  private long id;

  public MulticastPeer(int mPort, InetAddress mGroup, long id) throws IOException {
    initMulticast(mPort, mGroup, id);

    peerSocket = new DatagramSocket();
    peerSocket.setSoTimeout(timeout);
    peerAddress = peerSocket.getLocalAddress();
    peerPort = peerSocket.getLocalPort();
  }

  public MulticastPeer(int mPort, InetAddress mGroup, long id, int port) throws IOException {
    initMulticast(mPort, mGroup, id);

    peerSocket = new DatagramSocket(port);
    peerSocket.setSoTimeout(timeout);
    peerAddress = peerSocket.getLocalAddress();
    peerPort = peerSocket.getLocalPort();
  }

  public MulticastPeer(int mPort, InetAddress mGroup, long id, int port, InetAddress address) throws IOException {
    initMulticast(mPort, mGroup, id);

    peerSocket = new DatagramSocket(port, address);
    peerSocket.setSoTimeout(timeout);
    peerAddress = peerSocket.getLocalAddress();
    peerPort = peerSocket.getLocalPort();
  }

  private void initMulticast(int mPort, InetAddress mGroup, long id) throws IOException {
    this.multicastPort = mPort;
    this.multicastGroup = mGroup;
    this.id = id;

    multicastSocket = new MulticastSocket(multicastPort);
    multicastSocket.joinGroup(multicastGroup);
    multicastSocket.setSoTimeout(timeout);
    multicastSocket.setTimeToLive(1);
  }

  public void die() throws IOException {
    multicastSocket.leaveGroup(multicastGroup);
    multicastSocket.close();
    multicastSocket = null;

    peerSocket.close();
    peerSocket = null;
  }

  public void send(String message, InetAddress destAddress, int destPort) throws IOException {
    byte[] buffer = message.getBytes();
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length, destAddress, destPort);
    peerSocket.send(packet);
  }

  public DatagramPacket receive() throws IOException {
    byte[] buffer = new byte[4096];
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
    peerSocket.receive(packet);
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
}
