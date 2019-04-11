package dbs.protocols;

import dbs.TestPeer;
import dbs.TestRMI;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBackup {

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
  public void testBackupRequest() throws Exception {
    TestRMI.startRMIRegistry();
    Process peer1 = TestPeer.launchPeer("1.0",1, "peer_1");
    assertTrue(peer1.isAlive());
    Process backupRequest = this.launchBackupRequest("peer_1", "test_files/file_1.txt", 3);
    assertTrue(backupRequest.isAlive());
    TimeUnit.SECONDS.sleep(5);
    TestRMI.destroyRMIRegistry();
    peer1.destroy();
    backupRequest.destroy();
  }

  @Test
  public void test1() throws Exception {

    TestRMI.startRMIRegistry();

    // backup request
    Process backupRequest = this.launchBackupRequest("peer_1", "test_files/file_1.txt", 3);

    // peers
    Process peer_1 = TestPeer.launchPeer("1.0", 1, "peer_1");
    Process peer_2 = TestPeer.launchPeer("1.0", 2, "peer_2");
    Process peer_3 = TestPeer.launchPeer("1.0", 3, "peer_3");

    TimeUnit.SECONDS.sleep(5);
    // destroy processes
    backupRequest.destroy();
    peer_1.destroy();
    peer_2.destroy();
    peer_3.destroy();
    TestRMI.destroyRMIRegistry();
  }
}
