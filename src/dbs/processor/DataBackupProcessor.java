package dbs.processor;

import dbs.Multicaster;
import dbs.Peer;
import dbs.Protocol;
import dbs.Utils;
import dbs.fileInfoManager.FileInfoManager;
import dbs.message.Message;
import dbs.message.MessageException;
import dbs.message.MessageType;
import dbs.transmitter.StoredTransmitter;

import java.net.DatagramPacket;
import java.util.concurrent.TimeUnit;
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
        default:
          Peer.log("Dropped message from channel MDB", Level.INFO);
      }
    }

    private void processPutchunkMessage(Message m) {
      Peer.log("Received PUTCHUNK from " + m.getSenderId(), Level.INFO);
      String fileId = m.getFileId();
      int chunkNumber = m.getChunkNo();
      Long senderId = Long.parseLong(m.getSenderId());
      int desiredReplicationDegree = m.getReplication();
      String version = m.getVersion();
      byte[] chunk;

      try {
        chunk = m.getBody();
      } catch (IllegalStateException e) {
        Peer.log("Could not process the chunk content in the PUTCHUNK message",
            Level.SEVERE);
        return;
      }

      // the same peer has the one processing the message
      if (senderId == Peer.getInstance().getId())
        return;

      int replicationDegree =
          FileInfoManager.getInstance().getChunkReplicationDegree(fileId,
              chunkNumber);
      if (replicationDegree >= desiredReplicationDegree)
        return;

      storeChunk(fileId, chunkNumber, chunk);
      sendStoredMessage(version, fileId, chunkNumber);
      FileInfoManager.getInstance().addBackupPeer(fileId, chunkNumber,
          Peer.getInstance().getId());
      FileInfoManager.getInstance().setDesiredReplicationDegree(fileId,
          desiredReplicationDegree);
    }

    private void storeChunk(String fileId, int chunkNumber, byte[] chunk) {
      FileInfoManager.getInstance().storeChunk(fileId, chunkNumber, chunk);
    }

    private void sendStoredMessage(String version, String fileId, int chunkNumber) {
      Peer.getInstance().getPool().schedule(new StoredTransmitter(version,
              fileId,
              chunkNumber),
          Utils.getRandom(Protocol.minDelay, Protocol.maxDelay),
          TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public final Runnable runnable(DatagramPacket packet) {
    return new DataBackupRunnable(packet);
  }
}
