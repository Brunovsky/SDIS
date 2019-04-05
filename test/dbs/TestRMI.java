package dbs;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class TestRMI {

  private static Process RMIREGISTRY;

  private void startRMIRegistry() throws IOException {
    ProcessBuilder pb = new ProcessBuilder("rmiregistry");
    pb.directory(new File("out/production/SDIS_1819"));
    RMIREGISTRY = pb.start();
  }

  private void destroyRMIRegistry() {
    RMIREGISTRY.destroy();
  }

  @Test
  public void testRunRMIRegistry() throws IOException, InterruptedException {
    startRMIRegistry();
    assertTrue(RMIREGISTRY.isAlive());
    destroyRMIRegistry();
    TimeUnit.MILLISECONDS.sleep(500);
    assertTrue(!RMIREGISTRY.isAlive());
  }

}
