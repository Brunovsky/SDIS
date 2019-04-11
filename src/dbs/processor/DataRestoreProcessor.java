package dbs.processor;

import dbs.Peer;
import dbs.message.Message;
import dbs.message.MessageException;
import dbs.Multicaster;
import dbs.message.MessageType;
import dbs.transmitter.RestoreHandler;
import org.jetbrains.annotations.NotNull;

import java.net.DatagramPacket;
import java.util.logging.Level;

public class DataRestoreProcessor implements Multicaster.Processor {

  private class DataRestoreRunnable implements Runnable {
    private final DatagramPacket packet;

    DataRestoreRunnable(@NotNull DatagramPacket packet) {
      this.packet = packet;
    }

    @Override
    public void run() {
      try {
        Message m = new Message(packet);
        String senderId = Long.toString(Peer.getInstance().getId());
        if (senderId.equals(m.getSenderId())) return;
        this.processMessage(m);
      } catch (MessageException e) {
        Peer.log("Dropped message from channel MDR", Level.INFO);
      }
    }

    private void processMessage(Message m) {
      MessageType messageType = m.getType();
      switch (messageType) {
        case CHUNK:
          this.processChunkMessage(m);
          break;
        default:
          Peer.log("Dropped message from channel MDR", Level.INFO);
      }
    }

    private void processChunkMessage(Message m) {
      Peer.log("Received CHUNK from " + m.getSenderId(), Level.FINE);
      RestoreHandler.getInstance().receiveCHUNK(m);
    }
  }

  @Override
  public final Runnable runnable(@NotNull DatagramPacket packet) {
    return new DataRestoreRunnable(packet);
  }
}