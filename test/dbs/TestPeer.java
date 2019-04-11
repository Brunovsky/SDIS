package dbs;

import dbs.message.Message;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPeer {
  @Test
  void visualize() throws Exception {
    Protocol.mc = new MulticastChannel(InetAddress.getByName("237.0.0.1"), 29500);
    Protocol.mdb = new MulticastChannel(InetAddress.getByName("237.0.0.2"), 29501);
    Protocol.mdr = new MulticastChannel(InetAddress.getByName("237.0.0.3"), 29502);

    String hash1 = "1000000000000000000000000000000000000000000000000000000000000000";
    String hash2 = "2000000000000000000000000000000000000000000000000000000000000000";
    String hash3 = "3000000000000000000000000000000000000000000000000000000000000000";
    String hash4 = "4000000000000000000000000000000000000000000000000000000000000000";
    String hash5 = "5000000000000000000000000000000000000000000000000000000000000000";
    String hash6 = "6000000000000000000000000000000000000000000000000000000000000000";
    String hash7 = "7000000000000000000000000000000000000000000000000000000000000000";
    String hash8 = "8000000000000000000000000000000000000000000000000000000000000000";
    byte[] body1 = "Body 1 -- text -- 20".getBytes();
    byte[] body2 = "Body 2 -- text -- text -- 28".getBytes();
    byte[] body3 = "Body 3 -- text -- text -- text -- 36".getBytes();
    byte[] body4 = "Body 4 -- text -- text -- text -- text -- 44".getBytes();

    // Send Constructors
    Message m1 = Message.PUTCHUNK(hash1, "1.0", 0, 7, body1);
    Message m2 = Message.STORED(hash2, "1.0", 7);
    Message m3 = Message.GETCHUNK(hash3, "1.0", 17);
    Message m4 = Message.CHUNK(hash4, "1.0", 73, body2);
    Message m5 = Message.DELETE(hash5, "1.0");
    Message m6 = Message.REMOVED(hash6, "1.0", 4);
    Message m7 = Message.PUTCHUNK(hash7, "1.0", 63, 2, body3);
    Message m8 = Message.CHUNK(hash8, "1.0", 930, body4);

    Peer peer = new Peer(1337, "peer1337");

    peer.send(m1);
    peer.send(m2);
    peer.send(m3);
    peer.send(m4);
    peer.send(m5);
    peer.send(m6);
    peer.send(m7);
    peer.send(m8);
    Thread.sleep(300, 0); // let all messages go through

    peer.finish();
  }

  public static Process launchPeer(String protocolVersion, long id, String accessPoint) throws IOException {
    Protocol.mc = new MulticastChannel(InetAddress.getByName("237.0.0.1"), 29500);
    Protocol.mdb = new MulticastChannel(InetAddress.getByName("237.0.0.2"), 29501);
    Protocol.mdr = new MulticastChannel(InetAddress.getByName("237.0.0.3"), 29502);

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

  @Test
  public void testLaunchPeer() throws Exception {
    Protocol.mc = new MulticastChannel(InetAddress.getByName("237.0.0.1"), 29500);
    Protocol.mdb = new MulticastChannel(InetAddress.getByName("237.0.0.2"), 29501);
    Protocol.mdr = new MulticastChannel(InetAddress.getByName("237.0.0.3"), 29502);
    TestRMI.startRMIRegistry();
    Process peer1 = launchPeer("1.0",1, "peer_1");
    Process peer2 = launchPeer("1.0", 2, "peer_1");
    assertTrue(peer1.isAlive());
    assertTrue(peer2.isAlive());
    peer2.waitFor();
    assertEquals(1, peer2.exitValue()); // no two peers with the same access point
    assertTrue(peer1.isAlive());
    peer1.destroy();
    TestRMI.destroyRMIRegistry();
  }
}
