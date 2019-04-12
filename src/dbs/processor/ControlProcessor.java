package dbs.processor;

import dbs.Configuration;
import dbs.Peer;
import dbs.fileInfoManager.FileInfoManager;
import dbs.message.Message;
import dbs.message.MessageException;
import dbs.Multicaster;
import dbs.message.MessageType;
import dbs.transmitter.ReclaimHandler;
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
      ReclaimHandler.getInstance().receiveREMOVED(m);
    }

    private void processDeleteMessage(Message m) {
      Peer.log("Received DELETE from " + m.getSenderId(), Level.FINE);
      String fileId = m.getFileId();
      boolean sendDeletedMessage = FileInfoManager.getInstance().hasFileInfo(fileId);
      Peer.log("Received delete message for the file with id " + fileId, Level.INFO);
      if(!FileInfoManager.getInstance().deleteBackedUpFile(fileId)) {
        Peer.log("Could not delete the backed up file with id " + fileId, Level.SEVERE);
        return;
      }
      if(sendDeletedMessage) this.sendDeletedMessage(fileId, Configuration.version);
    }

    private void sendDeletedMessage(String fileId, String version) {
      Message deletedMessage = Message.DELETED(fileId, version);
      deletedMessage.setSenderId(Long.toString(Peer.getInstance().getId()));
      Peer.log("Going to send deleted message for the file with id " + fileId, Level.INFO);
      Peer.getInstance().send(deletedMessage);
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
