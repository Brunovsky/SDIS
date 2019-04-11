package dbs.processor;

import dbs.Peer;
import dbs.fileInfoManager.FileInfoManager;
import dbs.message.Message;
import dbs.message.MessageException;
import dbs.Multicaster;
import dbs.message.MessageType;
import dbs.transmitter.RestoreHandler;
import org.jetbrains.annotations.NotNull;

import java.net.DatagramPacket;
import java.util.logging.Level;

public class ControlProcessor implements Multicaster.Processor {

  private class ControlRunnable implements Runnable {
    private final DatagramPacket packet;

    ControlRunnable(@NotNull DatagramPacket packet) {
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
        Peer.log("Dropped message from channel MC", Level.INFO);
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
          Peer.log("Dropped message from channel MC", Level.INFO);
      }
    }

    private void processRemovedMessage(Message m) {
      Peer.log("Received REMOVED from " + m.getSenderId(), Level.FINE);
      // TODO...
    }

    private void processDeleteMessage(Message m) {
      Peer.log("Received DELETE from " + m.getSenderId(), Level.FINE);
      // TODO...
    }

    private void processGetchunkMessage(Message m) {
      Peer.log("Received GETCHUNK from " + m.getSenderId(), Level.FINE);
      RestoreHandler.getInstance().receiveGETCHUNK(m);
    }

    private void processStoredMessage(Message m) {
      Peer.log("Received STORED from " + m.getSenderId(), Level.FINE);
      Long senderId = Long.parseLong(m.getSenderId());
      String fileId = m.getFileId();
      Integer chunkNumber = m.getChunkNo();
      FileInfoManager.getInstance().addBackupPeer(fileId, chunkNumber, senderId);
    }
  }

  @Override
  public final Runnable runnable(@NotNull DatagramPacket packet) {
    return new ControlRunnable(packet);
  }
}
