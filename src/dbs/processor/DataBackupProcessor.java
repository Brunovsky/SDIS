package dbs.processor;

import dbs.Multicaster;
import dbs.Peer;
import dbs.message.Message;
import dbs.message.MessageException;
import org.jetbrains.annotations.NotNull;

import java.net.DatagramPacket;

public class DataBackupProcessor implements Multicaster.Processor {
  private class DataBackupRunnable implements Runnable {
    private DatagramPacket packet;
    private Peer peer;

    DataBackupRunnable(@NotNull DatagramPacket packet, Peer peer) {
      this.packet = packet;
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
  public final Runnable runnable(@NotNull DatagramPacket packet, Peer peer) {
    return new DataBackupRunnable(packet, peer);
  }
}
