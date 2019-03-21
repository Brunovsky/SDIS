package dbs;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

public class UdpChannel {
  private DatagramSocket socket;
  private InetAddress address;
  private int port;

  private Peer peer;

  public UdpChannel(Peer peer, int port, InetAddress address) throws IOException {
    this.peer = peer;
    socket = new DatagramSocket(port, address);
    socket.setSoTimeout(Protocol.udpTimeout);
    this.port = socket.getLocalPort();
    this.address = socket.getLocalAddress();
  }

  public UdpChannel(Peer peer, int port) throws IOException {
    this.peer = peer;
    socket = new DatagramSocket(port);
    socket.setSoTimeout(Protocol.udpTimeout);
    this.port = socket.getLocalPort();
    this.address = socket.getLocalAddress();
  }

  public UdpChannel(Peer peer) throws IOException {
    this.peer = peer;
    socket = new DatagramSocket();
    socket.setSoTimeout(Protocol.udpTimeout);
    this.port = socket.getLocalPort();
    this.address = socket.getLocalAddress();
  }

  public void die() throws IOException {
    socket.close();
    socket = null;
  }

  public void send(Message message, int toPort, InetAddress toAddress)
      throws IOException {
    socket.send(message.getPacket(peer.id, toPort, toAddress));
  }

  public Message receive() throws IOException {
    byte[] buffer = new byte[Protocol.maxPacketSize];
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
    socket.receive(packet);
    try {
      return new Message(packet);
    } catch (MessageException e) {
      System.err.println("Socket received invalid message:\n" + e.getMessage());
      return receive();
    }
  }

  public int port() {
    return this.port;
  }

  public InetAddress address() {
    return this.address;
  }

  public boolean setSoTimeout(int timeout) {
    try {
      socket.setSoTimeout(timeout);
      return true;
    } catch (SocketException e) {
      return false;
    }
  }
}
