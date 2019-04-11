package dbs.processor;

import dbs.Multicaster;
import dbs.Peer;
import dbs.Protocol;
import dbs.Utils;
import dbs.message.Message;
import dbs.message.MessageException;
import dbs.transmitter.StoredTransmitter;
import org.jetbrains.annotations.NotNull;

import java.net.DatagramPacket;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class DataBackupProcessor implements Multicaster.Processor {

  private class DataBackupRunnable implements Runnable {
    private final DatagramPacket packet;
    private final Peer peer;

    DataBackupRunnable(@NotNull DatagramPacket packet, @NotNull Peer peer) {
      this.packet = packet;
      this.peer = peer;
    }

    @Override
    public final void run() {
      try {
        Message m = new Message(packet);
        this.processPutchunkMessage(m);
      } catch (MessageException e) {
        System.err.print("[MDB Processor ERR] Invalid:\n" + e.getMessage() + "\n");
      }
    }

    private void processPutchunkMessage(Message m) {
      String fileId = m.getFileId();
      int chunkNumber = m.getChunkNo();
      Long senderId = Long.parseLong(m.getSenderId());
      int desiredReplicationDegree = m.getReplication();
      String version = m.getVersion();
      byte[] chunk = null;

      try {
        chunk = m.getBody();
      } catch(IllegalStateException e) {
        Peer.log("Could not process the chunk content in the PUTCHUNK message", Level.SEVERE);
        return;
      }

      if(senderId == this.peer.getId()) // the same peer has the one processing the message
        return;

      int replicationDegree = this.peer.fileInfoManager.getChunkReplicationDegree(fileId, chunkNumber);
      if(replicationDegree >= desiredReplicationDegree)
        return;

      storeChunk(fileId, chunkNumber, chunk);
      sendStoredMessage(version, fileId, chunkNumber);
      this.peer.fileInfoManager.addBackupPeer(fileId, chunkNumber, this.peer.getId());
      this.peer.fileInfoManager.setDesiredReplicationDegree(fileId, desiredReplicationDegree);
    }

    private void storeChunk(String fileId, int chunkNumber, byte[] chunk) {
      this.peer.fileInfoManager.storeChunk(fileId, chunkNumber, chunk);
    }

    private void sendStoredMessage(String version, String fileId, int chunkNumber) {
      this.peer.getPool().schedule(new StoredTransmitter(version, this.peer, fileId, chunkNumber),
              Utils.getRandom(Protocol.minDelay, Protocol.maxDelay),
              TimeUnit.MILLISECONDS);
    }

  }

  @Override
  public final Runnable runnable(@NotNull DatagramPacket packet, @NotNull Peer peer) {
    return new DataBackupRunnable(packet, peer);
  }
}
