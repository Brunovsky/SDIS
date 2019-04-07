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

  public void startRMIRegistry() throws Exception {
    ProcessBuilder pb = new ProcessBuilder("rmiregistry");
    pb.directory(new File("out/production/SDIS_1819"));
    RMIREGISTRY = pb.start();
    TimeUnit.SECONDS.sleep(1);
    assertTrue(RMIREGISTRY.isAlive());
  }

  public void destroyRMIRegistry() {
    RMIREGISTRY.destroy();
  }

  private Process launchPeer(String protocolVersion, long id, String accessPoint) throws IOException {
    ProcessBuilder pb = new ProcessBuilder();
    pb.command("java",
            "dbs.Peer",
            protocolVersion,
            Long.toString(id),
            accessPoint,
            Protocol.mc.getAddress().toString(),
            Integer.toString(Protocol.mc.getPort()),
            Protocol.mdb.getAddress().toString(),
            Integer.toString(Protocol.mdb.getPort()),
            Protocol.mdr.getAddress().toString(),
            Integer.toString(Protocol.mdr.getPort()));
    pb.directory(new File("out/production/SDIS_1819"));
    return pb.start();
  }

  private Process launchBackupRequest(String peerAccessPoint, String pathname, int replicationDegree) throws IOException {
    ProcessBuilder pb = new ProcessBuilder();
    pb.command("java",
            "dbs.TestApp",
            peerAccessPoint,
            "BACKUP",
            pathname,
            Integer.toString(replicationDegree));
    pb.directory(new File("out/production/SDIS_1819"));
    return pb.start();
  }

  @Test
  public void testRunRMIRegistry() throws Exception {
    startRMIRegistry();
    destroyRMIRegistry();
    TimeUnit.MILLISECONDS.sleep(500);
    assertTrue(!RMIREGISTRY.isAlive());
  }

  @Test
  public void testLaunchPeer() throws Exception {
    Protocol.mc = new MulticastChannel(InetAddress.getByName("237.0.0.1"), 29500);
    Protocol.mdb = new MulticastChannel(InetAddress.getByName("237.0.0.2"), 29501);
    Protocol.mdr = new MulticastChannel(InetAddress.getByName("237.0.0.3"), 29502);
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

  @Test
  public void testBackupRequest() throws Exception {
    startRMIRegistry();
    Process peer1 = launchPeer("1.0",1, "peer_1");
    assertTrue(peer1.isAlive());
    Process backupRequest = launchBackupRequest("peer_1", "test_files/file_1.txt", 3);
    TimeUnit.SECONDS.sleep(5);
    destroyRMIRegistry();
    peer1.destroy();
    backupRequest.destroy();
  }
}
