package dbs.processor;

import dbs.Peer;
import dbs.message.Message;
import dbs.message.MessageException;
import dbs.Multicaster;
import dbs.message.MessageType;
import org.jetbrains.annotations.NotNull;

import java.net.DatagramPacket;

public class DataRestoreProcessor implements Multicaster.Processor {
  private class DataRestoreRunnable implements Runnable {
    private final DatagramPacket packet;
    private final Peer peer;

    DataRestoreRunnable(@NotNull DatagramPacket packet, @NotNull Peer peer) {
      this.packet = packet;
      this.peer = peer;
    }

    @Override
    public void run() {
      try {
        Message m = new Message(packet);
        this.processMessage(m);
      } catch (MessageException e) {
        Peer.LOGGER.info("Dropped message from channel MDR");
      }
    }

    private void processMessage(Message m) {
      MessageType messageType = m.getType();
      switch (messageType) {
        case CHUNK:
          peer.getRestoreHandler().receiveCHUNK(m);
          break;
        default:
          Peer.LOGGER.info("Dropped message from channel MDR");
      }
    }
  }

  @Override
  public final Runnable runnable(@NotNull DatagramPacket packet, @NotNull Peer peer) {
    return new DataRestoreRunnable(packet, peer);
  }
}