package dbs;

import org.junit.jupiter.api.Test;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
