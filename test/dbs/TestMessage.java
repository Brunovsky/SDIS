package dbs;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.*;

class TestMessage {
  @Test
  void visualize() throws MessageException, UnknownHostException {
    Message message;

    InetAddress address = InetAddress.getByName("localhost");
    String hash1 = "abcdefabcdefabcdefabcdefabcdefabcdef012345670123456701234567ABCD";
    String hash2 = "ABCDABCDabcdabcd01230123012301239876987698769876aecbaecbaecb1357";

    message = Message.STORED(hash1, 0);
    message.setSenderId("1337");
    message.setAddress(address);
    message.setPort(29500);
    System.out.println(message.toString());

    message = Message.GETCHUNK(hash2, 7);
    message.setSenderId("808");
    message.setAddress(address);
    message.setPort(29600);
    System.out.println(message.toString());

    String body1 = "O rato roeu a\nrolha da garrafa\r\n do rei da Russia";
    String body2 = "Lorem ipsum dolor\r\nsit amet quorum\n";

    message = Message.CHUNK(hash2, 4, body1.getBytes());
    message.setSenderId("12345");
    System.out.println(message.toString());

    message = Message.DELETE(hash1);
    message.setSenderId("54321");
    System.out.println(message.toString());

    message = Message.REMOVED(hash1, 4);
    message.setSenderId("7890");
    System.out.println(message.toString());

    message = Message.PUTCHUNK(hash2, 9, 6, body2.getBytes());
    message.setSenderId("00000");
    System.out.println(message.toString());
  }

  @Test
  void constructorPUTCHUNK() throws MessageException, IOException {
    String hash1 = "23596404123595412495645951abcbdebafbebdbea1240623456943341324345";
    String hash2 = "ABCDABCDabcdabcd01230123012301239876987698769876aecbaecbaecb1357";
    String body1 = "O rato roeu a rolha\r\n\r\nda garrafa do rei da Russia ok ok\r\n";
    String body2 = "file file file abcd\r\n file 1234\n 1234 01234 ";
    for (int i = 0; i < 9; ++i) body2 += body2;
    String sender = "111111";
    String address = "localhost";
    int port = 7070;

    // Send Constructors
    Message m1 = Message.PUTCHUNK(hash1, 1,9, body1.getBytes());
    Message m2 = Message.PUTCHUNK(hash2, 2, 4, body2.getBytes());

    // Setters
    m1.setSenderId(sender);
    m2.setSenderId(sender);
    m2.setAddress(InetAddress.getByName(address));
    m2.setPort(port);

    // Getters
    assertEquals(sender, m2.getSenderId());
    assertEquals(InetAddress.getByName(address), m2.getAddress());
    assertEquals(port, m2.getPort());

    assertEquals(hash1, m1.getFileId());
    assertEquals(hash2, m2.getFileId());

    assertArrayEquals(body1.getBytes(), m1.getBody());
    assertArrayEquals(body2.getBytes(), m2.getBody());

    assertEquals(1, m1.getChunkNo());
    assertEquals(2, m2.getChunkNo());

    assertEquals(9, m1.getReplication());
    assertEquals(4, m2.getReplication());

    assertEquals(Protocol.version, m1.getVersion());
    assertEquals(Protocol.version, m2.getVersion());

    assertEquals(MessageType.PUTCHUNK, m1.getType());
    assertEquals(MessageType.PUTCHUNK, m2.getType());

    assertEquals(0, m1.getExtraHeaders().length);
    assertEquals(0, m2.getExtraHeaders().length);

    // Receive constructors
    Message tr1 = new Message(m1.getBytes());
    Message tr2 = new Message(m2.getBytes());

    assertEquals(tr1, m1);
    assertEquals(tr2, m2);
  }

  @Test
  void constructorSTORED() throws MessageException, UnknownHostException {
    String hash1 = "3245678765434256576543656123456754342655434344436214346244334545";
    String hash2 = "abdecbedabebcbebdebcbebdbabebce92348230534209570234abcebdbebceda";
    String sender = "222222";
    String address = "www.google.com";
    int port = 1337;

    // Send Constructors
    Message m1 = Message.STORED(hash1, 7);
    Message m2 = Message.STORED(hash2, 4);

    // Setters
    m1.setSenderId(sender);
    m2.setSenderId(sender);
    m2.setAddress(InetAddress.getByName(address));
    m2.setPort(port);

    // Getters
    assertEquals(sender, m2.getSenderId());
    assertEquals(InetAddress.getByName(address), m2.getAddress());
    assertEquals(port, m2.getPort());

    assertEquals(hash1, m1.getFileId());
    assertEquals(hash2, m2.getFileId());

    assertEquals(7, m1.getChunkNo());
    assertEquals(4, m2.getChunkNo());

    assertEquals(Protocol.version, m1.getVersion());
    assertEquals(Protocol.version, m2.getVersion());

    assertEquals(MessageType.STORED, m1.getType());
    assertEquals(MessageType.STORED, m2.getType());

    assertEquals(0, m1.getExtraHeaders().length);
    assertEquals(0, m2.getExtraHeaders().length);

    // Receive constructors
    Message tr1 = new Message(m1.getBytes());
    Message tr2 = new Message(m2.getBytes());

    assertEquals(tr1, m1);
    assertEquals(tr2, m2);
  }

  @Test
  void constructorGETCHUNK() throws MessageException, UnknownHostException {
    String hash1 = "5092350934759032493457230954309723940349572901409234572039470932";
    String hash2 = "3285238571290274584360934806120974539408932473485973245893458345";
    String sender = "333333";
    String address = "www.stackoverflow.com";
    int port = 12345;

    // Send Constructors
    Message m1 = Message.GETCHUNK(hash1, 2);
    Message m2 = Message.GETCHUNK(hash2, 5);

    // Setters
    m1.setSenderId(sender);
    m2.setSenderId(sender);
    m2.setAddress(InetAddress.getByName(address));
    m2.setPort(port);

    // Getters
    assertEquals(sender, m2.getSenderId());
    assertEquals(InetAddress.getByName(address), m2.getAddress());
    assertEquals(port, m2.getPort());

    assertEquals(hash1, m1.getFileId());
    assertEquals(hash2, m2.getFileId());

    assertEquals(2, m1.getChunkNo());
    assertEquals(5, m2.getChunkNo());

    assertEquals(Protocol.version, m1.getVersion());
    assertEquals(Protocol.version, m2.getVersion());

    assertEquals(MessageType.GETCHUNK, m1.getType());
    assertEquals(MessageType.GETCHUNK, m2.getType());

    assertEquals(0, m1.getExtraHeaders().length);
    assertEquals(0, m2.getExtraHeaders().length);

    // Receive constructors
    Message tr1 = new Message(m1.getBytes());
    Message tr2 = new Message(m2.getBytes());

    assertEquals(tr1, m1);
    assertEquals(tr2, m2);
  }

  @Test
  void constructorCHUNK() throws MessageException, UnknownHostException {
    String hash1 = "23596404123595412495645951abcbdebafbebdbea1240623456943341324345";
    String hash2 = "ABCDABCDabcda35ade90ce0d09a09cecebaa987698769876aecbaecbaecb1357";
    String body1 = "O rato roeu a rolha\r\r\nda garrafa do rei da Russia ok ok\r\n\r";
    String body2 = "file fi\0 file fi\0e file abcd\r\n file file 1234\n 1234 01234 ok";
    for (int i = 0; i < 7; ++i) body2 += body2;
    String sender = "444444";
    String address = "localhost";
    int port = 7070;

    // Send Constructors
    Message m1 = Message.CHUNK(hash1, 8, body1.getBytes());
    Message m2 = Message.CHUNK(hash2, 3, body2.getBytes());

    // Setters
    m1.setSenderId(sender);
    m2.setSenderId(sender);
    m2.setAddress(InetAddress.getByName(address));
    m2.setPort(port);

    // Getters
    assertEquals(sender, m2.getSenderId());
    assertEquals(InetAddress.getByName(address), m2.getAddress());
    assertEquals(port, m2.getPort());

    assertEquals(hash1, m1.getFileId());
    assertEquals(hash2, m2.getFileId());

    assertArrayEquals(body1.getBytes(), m1.getBody());
    assertArrayEquals(body2.getBytes(), m2.getBody());

    assertEquals(8, m1.getChunkNo());
    assertEquals(3, m2.getChunkNo());

    assertEquals(Protocol.version, m1.getVersion());
    assertEquals(Protocol.version, m2.getVersion());

    assertEquals(MessageType.CHUNK, m1.getType());
    assertEquals(MessageType.CHUNK, m2.getType());

    assertEquals(0, m1.getExtraHeaders().length);
    assertEquals(0, m2.getExtraHeaders().length);

    // Receive constructors
    Message tr1 = new Message(m1.getBytes());
    Message tr2 = new Message(m2.getBytes());

    assertEquals(tr1, m1);
    assertEquals(tr2, m2);
  }

  @Test
  void constructorDELETE() throws MessageException, UnknownHostException {
    String hash1 = "0692375093275349689103753408632957094863458703463485730285034523";
    String hash2 = "2214343905723905734390629304634668346345829037553094750235949390";
    String sender = "555555";
    String address = "localhost";
    int port = 6666;

    // Send Constructors
    Message m1 = Message.DELETE(hash1);
    Message m2 = Message.DELETE(hash2);

    // Setters
    m1.setSenderId(sender);
    m2.setSenderId(sender);
    m2.setAddress(InetAddress.getByName(address));
    m2.setPort(port);

    // Getters
    assertEquals(sender, m2.getSenderId());
    assertEquals(InetAddress.getByName(address), m2.getAddress());
    assertEquals(port, m2.getPort());

    assertEquals(hash1, m1.getFileId());
    assertEquals(hash2, m2.getFileId());

    assertEquals(Protocol.version, m1.getVersion());
    assertEquals(Protocol.version, m2.getVersion());

    assertEquals(MessageType.DELETE, m1.getType());
    assertEquals(MessageType.DELETE, m2.getType());

    assertEquals(0, m1.getExtraHeaders().length);
    assertEquals(0, m2.getExtraHeaders().length);

    // Receive constructors
    Message tr1 = new Message(m1.getBytes());
    Message tr2 = new Message(m2.getBytes());

    assertEquals(tr1, m1);
    assertEquals(tr2, m2);
  }

  @Test
  void constructorREMOVED() throws MessageException, IOException {
    String hash1 = "3456765435672482457389472385689124423058430230534534534809124723";
    String hash2 = "2342395079305790214092357349057091290404638952930474294893420712";
    String sender = "666666";
    String address = "www.stackoverflow.com";
    int port = 12345;

    // Send Constructors
    Message m1 = Message.REMOVED(hash1, 9);
    Message m2 = Message.REMOVED(hash2, 0);

    // Setters
    m1.setSenderId(sender);
    m2.setSenderId(sender);
    m2.setAddress(InetAddress.getByName(address));
    m2.setPort(port);

    // Getters
    assertEquals(sender, m2.getSenderId());
    assertEquals(InetAddress.getByName(address), m2.getAddress());
    assertEquals(port, m2.getPort());

    assertEquals(hash1, m1.getFileId());
    assertEquals(hash2, m2.getFileId());

    assertEquals(9, m1.getChunkNo());
    assertEquals(0, m2.getChunkNo());

    assertEquals(Protocol.version, m1.getVersion());
    assertEquals(Protocol.version, m2.getVersion());

    assertEquals(MessageType.REMOVED, m1.getType());
    assertEquals(MessageType.REMOVED, m2.getType());

    assertEquals(0, m1.getExtraHeaders().length);
    assertEquals(0, m2.getExtraHeaders().length);

    // Receive constructors
    Message tr1 = new Message(m1.getBytes());
    Message tr2 = new Message(m2.getBytes());

    assertEquals(tr1, m1);
    assertEquals(tr2, m2);
  }

  @Test
  void constructorPacket() throws MessageException, IOException {
    String hash1 = "3456765435672482457389472385689124423058430230534534534809124723";
    String hash2 = "54655346542352abdebcebdbaebbcbebde234234bacbed132842babcab123124";
    String hash3 = "2348346790239523095720357234095834579230534906230578340234857034";
    String hash4 = "1243464575612423087238520913713904723095791024723047023785348513";
    String hash5 = "0983409790870470572034650847357320460870589623405630425089702357";
    String hash6 = "3063496823933423349032709029347203941828496345328523095203753204";
    byte[] body1 = "Some random\r\nbody\n".getBytes();
    byte[] body4 = "Clear body with some text la la la la".getBytes();
    String sender1 = "11111", sender2 = "22222", sender3 = "33333";
    String sender4 = "44444", sender5 = "55555", sender6 = "66666";
    String add1 = "www.microsoft.com", add2 = "google.com", add3 = "fe.up.pt";
    String add4 = "github.com", add5 = "localhost", add6 = "moodle.up.pt";
    int p1 = 7878, p2 = 12212, p3 = 8080, p4 = 1337, p5 = 5123, p6 = 9351;

    Message m1 = Message.PUTCHUNK(hash1, 111, 9, body1);
    Message m2 = Message.STORED(hash2, 222);
    Message m3 = Message.GETCHUNK(hash3, 3333);
    Message m4 = Message.CHUNK(hash4, 55555, body4);
    Message m5 = Message.DELETE(hash5);
    Message m6 = Message.REMOVED(hash6, 6666);

    DatagramPacket packetB1 = m1.getPacket(sender1, p1, InetAddress.getByName(add1));
    DatagramPacket packetB2 = m2.getPacket(sender2, p2, InetAddress.getByName(add2));
    DatagramPacket packetB3 = m3.getPacket(sender3, p3, InetAddress.getByName(add3));
    DatagramPacket packetB4 = m4.getPacket(sender4, p4, InetAddress.getByName(add4));
    DatagramPacket packetB5 = m5.getPacket(sender5, p5, InetAddress.getByName(add5));
    DatagramPacket packetB6 = m6.getPacket(sender6, p6, InetAddress.getByName(add6));

    assertEquals(m1, new Message(packetB1));
    assertEquals(m2, new Message(packetB2));
    assertEquals(m3, new Message(packetB3));
    assertEquals(m4, new Message(packetB4));
    assertEquals(m5, new Message(packetB5));
    assertEquals(m6, new Message(packetB6));

    m1.setSenderId(sender1);
    m2.setSenderId(sender2);
    m3.setSenderId(sender3);
    m4.setSenderId(sender4);
    m5.setSenderId(sender5);
    m6.setSenderId(sender6);

    DatagramPacket packetA1 = m1.getPacket(p1, InetAddress.getByName(add1));
    DatagramPacket packetA2 = m2.getPacket(p2, InetAddress.getByName(add2));
    DatagramPacket packetA3 = m3.getPacket(p3, InetAddress.getByName(add3));
    DatagramPacket packetA4 = m4.getPacket(p4, InetAddress.getByName(add4));
    DatagramPacket packetA5 = m5.getPacket(p5, InetAddress.getByName(add5));
    DatagramPacket packetA6 = m6.getPacket(p6, InetAddress.getByName(add6));

    assertEquals(m1, new Message(packetA1));
    assertEquals(m2, new Message(packetA2));
    assertEquals(m3, new Message(packetA3));
    assertEquals(m4, new Message(packetA4));
    assertEquals(m5, new Message(packetA5));
    assertEquals(m6, new Message(packetA6));
  }

  @Test
  void badConstructors() {
    Class<MessageException> EXC = MessageException.class;
    Class<MessageError> ERR = MessageError.class;

    String hash1 = "3456765435672482457389472385689124423058430230534534534809124723";
    String hash2 = "54655346542352abdebcebdbaebbcbebde234234bacbed132842babcab123124";
    String badhash1 = "12434645756124230872385209137139q4723095791024723047023785348513";
    String badhash2 = "098340979087047057203465084757320460870589623405630425089702357";
    String badhash3 = "30634968239334233490327090291347203941828496345328523095203753204";
    byte[] body1 = "Some random\r\nbody\n".getBytes();
    byte[] body2 = "Clear body with some text la la la la".getBytes();
    String badsender1 = "manel1337", badsender2 = "~8080";
    String add1 = "www.microsoft.com", add2 = "google.com", add3 = "fe.up.pt";
    String add4 = "github.com", add5 = "localhost", add6 = "moodle.up.pt";
    int p1 = 7878, p2 = 12212, p3 = 8080, p4 = 1337, p5 = 5123, p6 = 9351;

    assertThrows(ERR, () -> Message.PUTCHUNK(hash1, 0, 13, body1));
    assertThrows(ERR, () -> Message.STORED(badhash1, 83));
    assertThrows(ERR, () -> Message.GETCHUNK(badhash3, 765));
    assertThrows(ERR, () -> Message.CHUNK(badhash2, 2, body2));
    assertThrows(ERR, () -> Message.DELETE(badhash2));
    assertThrows(ERR, () -> Message.REMOVED(hash2, -1));
    assertThrows(ERR, () -> Message.DELETE(hash1).setSenderId(badsender1));
    assertThrows(ERR, () -> Message.DELETE(hash1).setSenderId(badsender2));
  }
}
