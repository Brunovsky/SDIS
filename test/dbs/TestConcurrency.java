package dbs;

import dbs.message.Message;
import dbs.message.MessageException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.*;

public class TestConcurrency {
  private final class Printer implements Runnable {
    int id;

    private Printer(int id) {
      this.id = id;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(3000);
        System.out.print("Not interrupted! " + id + "\n");
      } catch (InterruptedException e) {
        System.out.print("Interrupted! " + id + "\n");
      }
    }
  }

  @Test
  void testCancel() throws InterruptedException {
    ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(5);

    Future f1 = pool.submit(new Printer(1));
    Future f2 = pool.submit(new Printer(2));
    Future f3 = pool.submit(new Printer(3));
    Future f4 = pool.submit(new Printer(4));
    Future f5 = pool.submit(new Printer(5));
    Future f6 = pool.submit(new Printer(6));
    Future f7 = pool.submit(new Printer(7));
    Future f8 = pool.submit(new Printer(8));
    Future f9 = pool.submit(new Printer(9));
    Future f10 = pool.submit(new Printer(10));
    Future f11 = pool.submit(new Printer(11));
    Future f12 = pool.submit(new Printer(12));
    Future f13 = pool.submit(new Printer(13));

    Thread.sleep(500);

    assertFalse(f1.isCancelled() || f1.isDone());
    assertFalse(f2.isCancelled() || f2.isDone());
    assertFalse(f3.isCancelled() || f3.isDone());
    assertFalse(f4.isCancelled() || f4.isDone());
    assertFalse(f5.isCancelled() || f5.isDone());
    assertFalse(f6.isCancelled() || f6.isDone());
    assertFalse(f7.isCancelled() || f7.isDone());

    f1.cancel(true);
    f2.cancel(true);
    f3.cancel(true);
    f4.cancel(false);
    f5.cancel(true);
    f6.cancel(true);

    assertTrue(f1.isCancelled());
    assertTrue(f2.isCancelled());
    assertTrue(f3.isCancelled());
    assertTrue(f4.isCancelled());
    assertTrue(f5.isCancelled());
    assertTrue(f6.isCancelled());

    Thread.sleep(6000);
  }

  String hash1 = "1000000000000000000000000000000000000000000000000000000000000000";

  @Test
  void testComms() throws IOException, MessageException {
    DatagramSocket d1 = new DatagramSocket(30001);
    MulticastSocket s2 = new MulticastSocket(29501);
    assertNotNull(d1);
    assertNotNull(s2);
    InetAddress address = InetAddress.getByName("230.0.0.1");
    s2.joinGroup(address);
    s2.setSoTimeout(Configuration.multicastTimeout);
    s2.setTimeToLive(1);

    Message sent = Message.STORED(hash1, "1.0", 7);
    DatagramPacket packet = sent.getPacket("1000",29501, address);
    d1.send(packet);

    byte[] buffer = new byte[65000];
    DatagramPacket rec = new DatagramPacket(buffer, buffer.length);
    s2.receive(rec);

    Message received = new Message(packet);
    System.out.println(sent);
    System.out.println(received);
  }

  public static void main(String[] args) throws InterruptedException {
    new TestConcurrency().testCancel();
  }
}
