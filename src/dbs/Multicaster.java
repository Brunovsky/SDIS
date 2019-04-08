package dbs;

import org.jetbrains.annotations.NotNull;

import java.io.IOError;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.util.logging.Logger;

public final class Multicaster implements Runnable {
  public interface Processor {
    Runnable runnable(@NotNull DatagramPacket packet, Peer peer);
  }

  private final static Logger LOGGER = Logger.getLogger(Multicaster.class.getName());

  private MulticastSocket socket;
  private final Peer peer;
  private final Processor processor;
  private boolean finished = false;
  private final MulticastChannel multicastChannel;

  /**
   * Die regularly by leaving the Multicast group and then closing the socket normally.
   * Idempotent operation.
   */
  private void die() {
    if (socket == null) return;
    finished = true;

    try {  // throws iff constructor throws, so this never throws.
      socket.leaveGroup(multicastChannel.getAddress());
      socket.close();
      socket = null;
    } catch (IOException e) {
      socket.close();
      socket = null;
    }
  }

  /**
   * Receive a packet from the multicast network.
   * If there is a reading timeout, it retries automatically until finished.
   *
   * @return The datagram packet read.
   */
  private DatagramPacket receive() {
    // TODO: Check if there is a problem here. This is a slight memory optimization.
    byte[] buffer = new byte[Protocol.maxPacketSize];
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

    try {
      socket.receive(packet);
      return packet;
    } catch (SocketTimeoutException e) {
      return null;
    } catch (IOException e) {
      if (socket.isClosed()) {
        throw new IOError(e);
      }
      System.err.println(e.getMessage());
      e.printStackTrace(System.err);
      return null;
    }
  }

  /**
   * Create a Multicaster for a given multicast group and port, with a message
   * processor chosen by the controlling peer.
   *
   * @param peer             The controlling peer
   * @param multicastChannel The Protocol Channel this Multicaster polls
   * @param processor        The processor of this Multicaster
   * @throws IOException If there is a problem setting up the multicast network, e.g.
   *                     invalid multicast address, port or timeout.
   */
  public Multicaster(@NotNull Peer peer, @NotNull MulticastChannel multicastChannel,
                     @NotNull Multicaster.Processor processor) throws IOException {
    this.peer = peer;
    this.multicastChannel = multicastChannel;
    this.processor = processor;
    try {
      socket = new MulticastSocket(multicastChannel.getPort());
      socket.joinGroup(multicastChannel.getAddress());
      socket.setSoTimeout(peer.getConfig().multicastTimeout);
      socket.setTimeToLive(1);
    } catch (IOException e) {
      LOGGER.severe("Could not create socket.\n");
      throw e;
    }
  }

  public final Processor processor() {
    return this.processor;
  }

  final void finish() {
    this.finished = true;
  }

  /**
   * Thread pool task. Receives packets from the multicast socket and forwards them to
   * threads in the peer's thread pool to parse and handle.
   * Does nothing if called once finished.
   */
  @Override
  public void run() {
    DatagramPacket packet;

    while (!finished) {
      packet = receive();
      if (packet == null) continue;

      peer.getPool().submit(processor.runnable(packet, this.peer));
    }

    die();
  }
}