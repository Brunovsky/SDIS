package dbs;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketException;

public class MulticastChannel {
  private MulticastSocket socket;
  private InetAddress group;
  private int port;

  private Peer peer;

  public MulticastChannel(Peer peer, int port, InetAddress group) throws IOException {
    this.peer = peer;
    this.port = port;
    this.group = group;

    socket = new MulticastSocket(port);
    socket.joinGroup(group);
    socket.setSoTimeout(Protocol.multicastTimeout);
    socket.setTimeToLive(1);
  }

  public void die() throws IOException {
    socket.leaveGroup(group);
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
      System.err.println("Multicast received invalid message:\n" + e.getMessage());
      return receive();
    }
  }

  public int port() {
    return this.port;
  }

  public InetAddress address() {
    return this.group;
  }

  public InetAddress group() {
    return this.group;
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
