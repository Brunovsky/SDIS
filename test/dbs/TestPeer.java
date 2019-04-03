package dbs;

import dbs.message.Message;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class TestPeer {
  @Test
  void visualize() throws IOException, InterruptedException {
    Protocol.setup();
    Peer peer = new Peer("1337");

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
    Message m1 = Message.PUTCHUNK(hash1, 0, 7, body1);
    Message m2 = Message.STORED(hash2, 7);
    Message m3 = Message.GETCHUNK(hash3, 17);
    Message m4 = Message.CHUNK(hash4, 73, body2);
    Message m5 = Message.DELETE(hash5);
    Message m6 = Message.REMOVED(hash6, 4);
    Message m7 = Message.PUTCHUNK(hash7, 63, 2, body3);
    Message m8 = Message.CHUNK(hash8, 930, body4);

    peer.start();

    peer.send(m1, Protocol.MC);
    peer.send(m2, Protocol.MDB);
    peer.send(m3, Protocol.MDR);
    peer.send(m4, Protocol.MC);
    peer.send(m5, Protocol.MC);
    peer.send(m6, Protocol.MDB);
    peer.send(m7, Protocol.MDB);
    peer.send(m8, Protocol.MDR);
    Thread.sleep(50, 0); // let all messages go through

    peer.finish();
  }
}
