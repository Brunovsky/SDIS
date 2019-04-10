package dbs;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.rmi.registry.Registry;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class TestRMI {
  private static Process RMIREGISTRY;

  public static void startRMIRegistry() throws Exception {
    ProcessBuilder pb = new ProcessBuilder("rmiregistry");
    pb.directory(new File("out/production/SDIS_1819"));
    RMIREGISTRY = pb.start();
    TimeUnit.SECONDS.sleep(1);
    assertTrue(RMIREGISTRY.isAlive());
  }

  public static void destroyRMIRegistry() {
    RMIREGISTRY.destroy();
  }

  @Test
  public void testRunRMIRegistry() throws Exception {
    startRMIRegistry();
    destroyRMIRegistry();
    TimeUnit.MILLISECONDS.sleep(500);
    assertTrue(!RMIREGISTRY.isAlive());
  }

  @Test
  public void testUtilsRegistry() {
    Registry registry = Utils.registry();
    assertNotNull(registry);
  }

  private final class GetRegistry implements Runnable {
    private String name;

    GetRegistry(String name) {
      this.name = name;
    }

    @Override
    public void run() {
      System.out.println("Tester " + name);
      Registry registry = Utils.registry();
      assertNotNull(registry);
    }
  }

  @Test
  public void testUtilsRegistryLoop() throws InterruptedException {
    Thread[] threads = new Thread[20];

    for (int i = 0; i < 20; ++i) {
      threads[i] = new Thread(new GetRegistry(Integer.toString(i)));
    }

    for (int i = 0; i < 20; ++i) {
      threads[i].start();
    }

    for (int i = 0; i < 20; ++i) {
      threads[i].join();
    }
  }
}
