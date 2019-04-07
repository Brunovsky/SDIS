package dbs.processor;

import dbs.Peer;
import dbs.message.Message;
import dbs.message.MessageException;
import dbs.Multicaster;
import org.jetbrains.annotations.NotNull;

import java.net.DatagramPacket;

public class ControlProcessor implements Multicaster.Processor {
  private class ControlRunnable implements Runnable {
    private final DatagramPacket packet;
    private final Peer peer;

    ControlRunnable(@NotNull DatagramPacket packet, @NotNull Peer peer) {
      this.packet = packet;
      this.peer = peer;
    }

    @Override
    public void run() {
      try {
        Message m = new Message(packet);
        System.out.println("[MC Processor] \n" + m.toString() + "\n");
      } catch (MessageException e) {
        System.err.println("[MC Processor ERR] Invalid:\n" + e.getMessage() + "\n");
      }
    }
  }

  @Override
  public final Runnable runnable(@NotNull DatagramPacket packet, @NotNull Peer peer) {
    return new ControlRunnable(packet, peer);
  }
}
