package dbs;


import dbs.processor.ControlProcessor;
import dbs.processor.DataBackupProcessor;
import dbs.processor.DataRestoreProcessor;
import org.jetbrains.annotations.NotNull;

import java.io.IOError;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;

public final class Multicaster implements Runnable {
  public interface Processor {
    Runnable runnable(@NotNull DatagramPacket packet);
    enum ProcessorType {CONTROL, DATA_BACKUP, DATA_RESTORE}
  }

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
   * @param processorType    The type of the message processor
   * @throws IOException If there is a problem setting up the multicast network, e.g.
   *                     invalid multicast address, port or timeout.
   */
  public Multicaster(@NotNull Peer peer, @NotNull MulticastChannel multicastChannel,
                     @NotNull Multicaster.Processor.ProcessorType processorType) throws Exception {
    this.peer = peer;
    this.multicastChannel = multicastChannel;
    this.processor = processorType == Processor.ProcessorType.CONTROL ? new ControlProcessor() :
        processorType == Processor.ProcessorType.DATA_BACKUP ? new DataBackupProcessor() : new DataRestoreProcessor();
    try {
      socket = new MulticastSocket(multicastChannel.getPort());
      socket.joinGroup(multicastChannel.getAddress());
      socket.setSoTimeout(Configuration.multicastTimeout);
      socket.setTimeToLive(1);
    } catch (Exception e) {
      Utils.printErr("Multicaster", "Could not create socket.");
      throw e;
    }
  }

  public final Processor processor() {
    return this.processor;
  }

  public final void finish() {
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

      peer.getPool().submit(processor.runnable(packet));
    }

    die();
  }
}