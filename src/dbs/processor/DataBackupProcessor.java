package dbs.processor;

import dbs.Multicaster;
import dbs.Peer;
import dbs.Utils;
import dbs.message.Message;
import dbs.message.MessageException;
import org.jetbrains.annotations.NotNull;

import java.net.DatagramPacket;
import java.util.concurrent.TimeUnit;

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
        this.peer.LOGGER.severe("Could not process the chunk content in the PUTCHUNK message.\n");
        return;
      }

      if(senderId == this.peer.getId()) // the same peer has the one processing the message
        return;

      /*int replicationDegree = this.peer.getReplicationDegree(fileId, chunkNumber);
      if(replicationDegree >= desiredReplicationDegree)
        return;

      storeChunk(fileId, chunkNumber, chunk);
      sendStoredMessage(version, fileId, chunkNumber);
      this.peer.insertIntoChunksReplicationDegreeHashMap(fileId, chunkNumber, this.peer.getId());*/
    }

    private void storeChunk(String fileId, int chunkNumber, byte[] chunk) {
      // TODO: store the chunk using the file manager
    }

    private void sendStoredMessage(String version, String fileId, int chunkNumber) {
      try {
        Utils.waitRandom(0, 400, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        this.peer.LOGGER.warning("Could not wait to send the STORED message.\n");
      }
      Message m = Message.STORED(fileId, version, chunkNumber);
      this.peer.send(m);
    }
  }

  @Override
  public final Runnable runnable(@NotNull DatagramPacket packet, @NotNull Peer peer) {
    return new DataBackupRunnable(packet, peer);
  }
}
