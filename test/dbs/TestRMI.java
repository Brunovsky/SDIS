package dbs;

import dbs.processor.ControlProcessor;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class TestRMI {

  private static Process RMIREGISTRY;
  private static Configuration conf;

  @Test
  public void startRMIRegistry() throws Exception {
    ProcessBuilder pb = new ProcessBuilder("rmiregistry");
    pb.directory(new File("out/production/SDIS_1819"));
    RMIREGISTRY = pb.start();
    TimeUnit.MILLISECONDS.sleep(1000);
    assertTrue(RMIREGISTRY.isAlive());
  }

  private void destroyRMIRegistry() {
    RMIREGISTRY.destroy();
  }

  private Process launchPeer(String protocolVersion, long id, String accessPoint) throws IOException {
    ProcessBuilder pb = new ProcessBuilder();
    pb.command("java",
            "dbs.Peer",
            protocolVersion,
            Long.toString(id),
            accessPoint,
            Protocol.mcAddress,
            Protocol.mcPort,
            Protocol.mdbAddress,
            Protocol.mdbPort,
            Protocol.mdrAddress,
            Protocol.mdrPort);
    pb.directory(new File("out/production/SDIS_1819"));
    return pb.start();
  }

  @Test
  public void testRunRMIRegistry() throws IOException, InterruptedException, Exception {
    startRMIRegistry();
    destroyRMIRegistry();
    TimeUnit.MILLISECONDS.sleep(500);
    assertTrue(!RMIREGISTRY.isAlive());
  }

  @Test
  public void testLaunchPeer() throws Exception {
    startRMIRegistry();
    Process peer1 = launchPeer("1.0",1, "peer_1");
    Process peer2 = launchPeer("1.0", 2, "peer_1");
    assertTrue(peer1.isAlive());
    assertTrue(peer2.isAlive());
    peer2.waitFor();
    assertEquals(1, peer2.exitValue()); // no two peers with the same access point
    assertTrue(peer1.isAlive());
    peer1.destroy();
    destroyRMIRegistry();
  }

}
