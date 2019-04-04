package dbs.processor;

import dbs.message.Message;
import dbs.message.MessageException;
import dbs.Multicaster;
import org.jetbrains.annotations.NotNull;

import java.net.DatagramPacket;

public class DataRestoreProcessor implements Multicaster.Processor {
  private class DataRestoreRunnable implements Runnable {
    private DatagramPacket packet;

    DataRestoreRunnable(@NotNull DatagramPacket packet) {
      this.packet = packet;
    }

    @Override
    public void run() {
      try {
        Message m = new Message(packet);
        System.out.print("[MDR Processor] \n" + m.toString() + "\n");
      } catch (MessageException e) {
        System.err.print("[MDR Processor ERR] Invalid:\n" + e.getMessage() + "\n");
      }
    }
  }

  @Override
  public final Runnable runnable(@NotNull DatagramPacket packet) {
    return new DataRestoreRunnable(packet);
  }
}