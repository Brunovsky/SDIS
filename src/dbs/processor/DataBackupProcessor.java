package dbs.processor;

import dbs.message.Message;
import dbs.message.MessageException;
import dbs.Multicaster;

import java.net.DatagramPacket;

public class DataBackupProcessor implements Multicaster.Processor {
  private class DataBackupRunnable implements Runnable {
    private DatagramPacket packet;

    DataBackupRunnable(DatagramPacket packet) {
      this.packet = packet;
    }

    @Override
    public final void run() {
      try {
        Message m = new Message(packet);
        System.out.print("[MDB Processor] \n" + m.toString() + "\n");
      } catch (MessageException e) {
        System.err.print("[MDB Processor ERR] Invalid:\n" + e.getMessage() + "\n");
      }
    }
  }

  @Override
  public final Runnable runnable(DatagramPacket packet) {
    return new DataBackupRunnable(packet);
  }
}
