package dbs;

import dbs.message.Message;

import java.io.IOError;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public final class PeerSocket implements Runnable {
  private DatagramSocket socket;
  private final LinkedBlockingDeque<DatagramPacket> queue;
  private boolean finished = false;
  // set to true to quit after next message.

  PeerSocket(int port, InetAddress address) throws IOException {
    this.socket = new DatagramSocket(port, address);
    this.queue = new LinkedBlockingDeque<>(Configuration.socketQueueCapacity);
  }

  PeerSocket(int port) throws IOException {
    this.socket = new DatagramSocket(port);
    this.queue = new LinkedBlockingDeque<>(Configuration.socketQueueCapacity);
  }

  PeerSocket() throws IOException {
    this.socket = new DatagramSocket();
    this.queue = new LinkedBlockingDeque<>(Configuration.socketQueueCapacity);
  }

  /**
   * Close the socket gracefully.
   */
  private void die() {
    if (socket == null) return;
    finished = true;

    socket.close();
    socket = null;
  }

  /**
   * Send this packet to the output socket.
   *
   * @param packet The datagram packet to be sent, taken from the front of the queue.
   */
  private void send(DatagramPacket packet) {
    try {
      socket.send(packet);
    } catch (IOException e) {
      if (socket.isClosed()) {
        throw new IOError(e);
      }

      // TODO: How to handle other exceptions?
      System.err.println(e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * Add this message to the output queue, destined to a specific channel.
   *
   * @param message The message to be sent
   * @param channel The destination channel
   */
  public void sendTo(Message message, MulticastChannel channel) {
    if (finished) return;
    String id = Long.toString(Peer.getInstance().getId());
    queue.add(message.getPacket(id, channel.getPort(), channel.getAddress()));
    Peer.log("Sending... " + message.shortText(), Level.INFO);
  }

  /**
   * Add this message to the output queue. Automatically detect the channel based on
   * the message type.
   *
   * @param message The message to be sent.
   */
  public void send(Message message) {
    if (finished) return;
    MulticastChannel destinationChannel;
    switch (message.getType()) {
    case PUTCHUNK:
      destinationChannel = Protocol.mdb;
      break;
    case CHUNK:
      destinationChannel = Protocol.mdr;
      break;
    default:
      destinationChannel = Protocol.mc;
      break;
    }
    sendTo(message, destinationChannel);
  }

  final void finish() {
    this.finished = true;
  }

  /**
   * Thread pool task. Dispatches packets added by other agents the output queue through
   * the public send* methods.
   * The construction of the datagram packet is made by the agents themselves.
   * Does nothing if called once finished.
   */
  @Override
  public void run() {
    DatagramPacket packet;

    while (!finished) {
      try {
        packet = queue.poll(Configuration.socketTimeout, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        continue;
      }
      if (packet == null) continue;
      send(packet);
    }

    while (!queue.isEmpty()) {
      packet = queue.pop();
      if (packet == null) continue;
      send(packet);
    }

    die();
  }
}
