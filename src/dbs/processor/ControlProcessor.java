package dbs.processor;

import dbs.Configuration;
import dbs.Multicaster;
import dbs.Peer;
import dbs.files.FileInfoManager;
import dbs.message.Message;
import dbs.message.MessageException;
import dbs.message.MessageType;
import dbs.transmitter.BackupHandler;
import dbs.transmitter.ReclaimHandler;
import dbs.transmitter.RestoreHandler;

import java.net.DatagramPacket;
import java.util.logging.Level;

public class ControlProcessor implements Multicaster.Processor {

  private class ControlRunnable implements Runnable {
    private final DatagramPacket packet;

    ControlRunnable(DatagramPacket packet) {
      this.packet = packet;
    }

    @Override
    public void run() {
      try {
        Message m = new Message(packet);
        String senderId = Long.toString(Peer.getInstance().getId());
        if (senderId.equals(m.getSenderId())) return;
        Peer.log("Received " + m.shortFrom(), Level.INFO);
        this.processMessage(m);
      } catch (MessageException e) {
        Peer.log("Dropped message from channel MC", e, Level.INFO);
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
        case REMOVED:
          this.processRemovedMessage(m);
          break;
        case DELETE:
          this.processDeleteMessage(m);
          break;
        case DELETED:
          this.processDeletedMessage(m);
          break;
        default:
          Peer.log("Dropped message from channel MC", Level.INFO);
      }
    }

    private void processStoredMessage(Message m) {
      BackupHandler.getInstance().receiveSTORED(m);
    }

    private void processGetchunkMessage(Message m) {
      RestoreHandler.getInstance().receiveGETCHUNK(m);
    }

    private void processRemovedMessage(Message m) {
      ReclaimHandler.getInstance().receiveREMOVED(m);
    }

    private void processDeleteMessage(Message m) {
      String fileId = m.getFileId();
      boolean sendDeletedMessage = FileInfoManager.getInstance().hasOtherFileInfo(fileId);
      FileInfoManager.getInstance().deleteOtherFileInfo(fileId);
      if(sendDeletedMessage) this.sendDeletedMessage(fileId, Configuration.version);
    }

    private void processDeletedMessage(Message m) {
      Long senderId = Long.parseLong(m.getSenderId());
      String fileId = m.getFileId();
      FileInfoManager.getInstance().removeBackupPeer(fileId, senderId);
    }

    private void sendDeletedMessage(String fileId, String version) {
      Message deletedMessage = Message.DELETED(fileId, version);
      Peer.log("Going to send deleted message for the file with id " + fileId, Level.INFO);
      Peer.getInstance().send(deletedMessage);
    }
  }

  @Override
  public final Runnable runnable(DatagramPacket packet) {
    return new ControlRunnable(packet);
  }
}
