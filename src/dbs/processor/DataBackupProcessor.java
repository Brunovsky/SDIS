package dbs.processor;

import dbs.Multicaster;
import dbs.Peer;
import dbs.message.Message;
import dbs.message.MessageException;
import dbs.message.MessageType;
import dbs.transmitter.BackupHandler;

import java.net.DatagramPacket;
import java.util.logging.Level;

public class DataBackupProcessor implements Multicaster.Processor {

  private class DataBackupRunnable implements Runnable {
    private final DatagramPacket packet;

    DataBackupRunnable(DatagramPacket packet) {
      this.packet = packet;
    }

    @Override
    public final void run() {
      try {
        Message m = new Message(packet);
        String senderId = Long.toString(Peer.getInstance().getId());
        if (senderId.equals(m.getSenderId())) return;
        this.processMessage(m);
      } catch (MessageException e) {
        Peer.log("Dropped message from channel MDB", Level.INFO);
      }
    }

    private void processMessage(Message m) {
      MessageType messageType = m.getType();
      switch (messageType) {
        case PUTCHUNK:
          this.processPutchunkMessage(m);
          break;
          // ...
        default:
          Peer.log("Dropped message from channel MDB", Level.INFO);
      }
    }

    private void processPutchunkMessage(Message m) {
      Peer.log("Received PUTCHUNK from " + m.getSenderId(), Level.INFO);
      BackupHandler.getInstance().receivePUTCHUNK(m);
    }
  }

  @Override
  public final Runnable runnable(DatagramPacket packet) {
    return new DataBackupRunnable(packet);
  }
}
