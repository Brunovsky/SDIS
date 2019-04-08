package dbs.processor;

import dbs.Peer;
import dbs.message.Message;
import dbs.message.MessageException;
import dbs.Multicaster;
import dbs.message.MessageType;
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
        this.processMessage(m);
        System.out.println("[MC Processor] \n" + m.toString() + "\n");
      } catch (MessageException e) {
        System.err.println("[MC Processor ERR] Invalid:\n" + e.getMessage() + "\n");
      }
    }

    private void processMessage(Message m) {
      MessageType messageType = m.getType();
      switch(messageType) {
        case STORED:
          this.processStoredMessage(m);
          break;
        case GETCHUNK:
          this.processGetchunkMessage(m);
          break;
        case DELETE:
          this.processDeleteMessage(m);
          break;
        case REMOVED:
          this.processRemovedMessage(m);
          break;
        default:
          this.peer.LOGGER.severe("Could not recognized received message. Unexpected message type '" + messageType.toString() + "' in the MC channel.\n");
          return;
      }
    }

    private void processRemovedMessage(Message m) {

    }

    private void processDeleteMessage(Message m) {

    }

    private void processGetchunkMessage(Message m) {
    }

    private void processStoredMessage(Message m) {
      // TODO: update hasmap
    }
  }

  @Override
  public final Runnable runnable(@NotNull DatagramPacket packet, @NotNull Peer peer) {
    return new ControlRunnable(packet, peer);
  }
}
