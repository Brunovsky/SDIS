package dbs.processor;

import dbs.Multicaster;
import dbs.Peer;
import dbs.message.Message;
import dbs.message.MessageException;
import org.jetbrains.annotations.NotNull;

import java.net.DatagramPacket;

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
      int chunkNo = m.getChunkNo();

      // TODO: continue. Ver livro de apontamentos.
    }
  }

  @Override
  public final Runnable runnable(@NotNull DatagramPacket packet, @NotNull Peer peer) {
    return new DataBackupRunnable(packet, peer);
  }
}
