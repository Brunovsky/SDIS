package dbs;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

// The header consists of a sequence of ASCII lines, sequences of ASCII codes terminated
// with the sequence '0xD''0xA', which we denote <CRLF> because these are the ASCII codes
// of the CR and LF chars respectively. Each header line is a sequence of fields,
// sequences of ASCII codes, separated by spaces, the ASCII char ' '. Note that:
//     there may be more than one space between fields;
//     there may be zero or more spaces after the last field in a line;
//     the header always terminates with an empty header line. I.e. the <CRLF> of the last
//     header line is followed immediately by another <CRLF>, white spaces included,
//     without any character in between.
// In the version described herein, the header has only the following non-empty single
// line:
// <MessageType> <Version> <SenderId> <FileId> <ChunkNo> <ReplicationDeg> <CRLF>
// Some of these fields may not be used by some messages, but all fields that appear in a
// message must appear in the relative order specified above.
// Next we describe the meaning of each field and its format.
// <MessageType>
//     This is the type of the message. Each subprotocol specifies its own message types.
//     This field determines the format of the message and what actions its receivers
//     should perform. This is encoded as a variable length sequence of ASCII characters.
// <Version>
//     This is the version of the protocol. It is a three ASCII char sequence with the
//     format <n>'.'<m>, where <n> and <m> are the ASCII codes of digits. For example,
//     version 1.0, the one specified in this document, should be encoded as the char
//     sequence '1''.''0'.
// <SenderId>
//     This is the id of the server that has sent the message. This field is useful in
//     many subprotocols. This is encoded as a variable length sequence of ASCII digits.
// <FileId>
//     This is the file identifier for the backup service. As stated above, it is supposed
//     to be obtained by using the SHA256 cryptographic hash function. As its name
//     indicates its length is 256 bit, i.e. 32 bytes, and should be encoded as a 64 ASCII
//     character sequence. The encoding is as follows: each byte of the hash value is
//     encoded by the two ASCII characters corresponding to the hexadecimal representation
//     of that byte. E.g., a byte with value 0xB2 should be represented by the two char
//     sequence 'B''2' (or 'b''2', it does not matter). The entire hash is represented in
//     big-endian order, i.e. from the MSB (byte 31) to the LSB (byte 0).
// <ChunkNo>
//     This field together with the FileId specifies a chunk in the file. The chunk
//     numbers are integers and should be assigned sequentially starting at 0. It is
//     encoded as a sequence of ASCII characters corresponding to the decimal
//     representation of that number, with the most significant digit first. The length of
//     this field is variable, but should not be larger than 6 chars. Therefore, each file
//     can have at most one million chunks. Given that each chunk is 64 KByte, this limits
//     the size of the files to backup to 64 GByte.
// <ReplicationDeg>
//     This field contains the desired replication degree of the chunk. This is a digit,
//     thus allowing a replication degree of up to 9. It takes one byte, which is the
//     ASCII code of that digit.
// Body
// When present, the body contains the data of a file chunk. The length of the body is
// variable. As stated above, if it is smaller than the maximum chunk size, 64KByte, it is
// the last chunk in a file. The protocol does not interpret the contents of the Body. For
// the protocol its value is just a byte sequence. You must not encode it, e.g. like is
// done with the FileId header field.

public class Message {
  private MessageType messageType;
  private String version;
  private String senderId;
  private String fileId;
  private int chunkNo;
  private int replication;

  private String[] more;
  private byte[] body;

  private InetAddress address;
  private int port;

  /**
   * * IO auxiliary functions, conversions
   */

  public String mainHeaderString() {
    String[] parts = new String[messageType.fields()];

    parts[0] = messageType.toString();
    parts[1] = version;
    parts[2] = senderId;
    parts[3] = fileId;
    if (messageType.fields() >= 5) parts[4] = Integer.toString(chunkNo);
    if (messageType.fields() >= 6) parts[5] = Integer.toString(replication);

    return String.join(" ", parts);
  }

  public String headerString() {
    String main = mainHeaderString();
    if (more.length > 0) {
      String rest = String.join("\r\n", more);
      main = String.join("\r\n", main, rest);
    }
    return main;
  }

  private byte[] getHeaderBytes() {
    return (headerString() + "\r\n\r\n").getBytes();
  }

  byte[] getBytes() {
    byte[] header = getHeaderBytes();
    System.out.println("Header length: " + header.length);

    if (body == null) return header;

    int length = header.length + body.length;
    byte[] bytes = new byte[length];

    System.arraycopy(header, 0, bytes, 0, header.length);
    System.arraycopy(body, 0, bytes, header.length, body.length);

    return bytes;
  }

  DatagramPacket getPacket(int port, InetAddress address) {
    byte[] bytes = getBytes();
    System.out.println("Get Packet length: " + bytes.length);
    return new DatagramPacket(bytes, bytes.length, address, port);
  }

  DatagramPacket getPacket(String senderId, int port, InetAddress address) {
    setSenderId(senderId);
    return getPacket(port, address);
  }

  /**
   * * Field validation functions
   */

  private void validateMainHeader(String[] parts) throws MessageException {
    if (parts.length == 0) {
      throw new MessageException("Main message header is empty");
    }

    MessageType type = MessageType.from(parts[0]);  // can throw too

    if (parts.length != type.fields()) {
      throw new MessageException("Incomplete message header for " + parts[0]
          + " message: expected " + type.fields()
          + " fields, got " + parts.length + ".");
    }
  }

  private void validateVersion(String version) throws MessageException {
    if (!version.matches("[0-9]\\.[0-9]")) {
      throw new MessageException("Invalid protocol version: " + version);
    }
  }

  private void validateSenderId(String senderId) throws MessageException {
    if (!senderId.matches("[0-9]+")) {
      throw new MessageException("Invalid sender id: " + senderId);
    }
  }

  private void validateFileId(String fileId) throws MessageException {
    if (fileId.length() != 64 || !fileId.matches("[a-fA-F0-9]+")) {
      throw new MessageException("Invalid file hash: " + fileId);
    }
  }

  private void validateChunkNo(String chunkNo) throws MessageException {
    if (!chunkNo.matches("[0-9]+")) {
      throw new MessageException("Invalid chunkNo: " + chunkNo);
    }
  }

  private void validateChunkNo(int chunkNo) throws MessageException {
    if (chunkNo < 0) {
      throw new MessageException("Invalid chunkNo: " + chunkNo);
    }
  }

  private void validateReplicationDegree(String replication) throws MessageException {
    if (!replication.matches("[0-9]")) {
      throw new MessageException("Invalid replication degree: " + replication);
    }
  }

  private void validateReplicationDegree(int replication) throws MessageException {
    if (replication < 0 || replication >= 10) {
      throw new MessageException("Replication degree should be 1..9: " + replication);
    }
  }

  /**
   * * Receive constructor auxiliary functions
   */

  /**
   * Parse a String corresponding to the first header line, and assign it to this message.
   * Weak exception guarantee (can be made strong)
   *
   * @param header header string
   */
  private void parseMainHeader(String header) throws MessageException {
    String[] parts = header.split(" ");

    validateMainHeader(parts);
    messageType = MessageType.from(parts[0]);

    if (parts.length >= 2) {
      validateVersion(parts[1]);
      version = parts[1];
    }

    if (parts.length >= 3) {
      validateSenderId(parts[2]);
      senderId = parts[2];
    }

    if (parts.length >= 4) {
      validateFileId(parts[3]);
      fileId = parts[3];
    }

    if (parts.length >= 5) {
      validateChunkNo(parts[4]);
      chunkNo = Integer.parseInt(parts[4]);
    }

    if (parts.length >= 6) {
      validateReplicationDegree(parts[5]);
      replication = Integer.parseInt(parts[5]);
    }
  }

  /**
   * Parse a collection of headers and assign it to this message.
   * Weak exception guarantee (can be made strong).
   *
   * @param headers The list of headers (v1.0 has just one)
   */
  private void parseHeaders(String[] headers) throws MessageException {
    if (headers.length == 0) {
      throw new MessageException("Message has no headers");
    }

    parseMainHeader(headers[0]);
    more = Arrays.copyOfRange(headers, 1, headers.length);
  }

  /**
   * * Constructors
   */

  /**
   * [RECEIVE] Constructs a message directly from a block of bytes, properly trimmed
   */
  Message(byte[] bytes) throws MessageException {
    try {
      // Find index of first occurrence of \r\n\r\n
      int index = new String(bytes, StandardCharsets.UTF_8).indexOf("\r\n\r\n");
      if (index < 0) {
        throw new MessageException("Invalid Message byte array: no header end present");
      }

      // Headers up to index, body after index
      byte[] headersBytes = Arrays.copyOfRange(bytes, 0, index);
      String[] headers = new String(headersBytes).split("\r\n");

      parseHeaders(headers);

      if (messageType.hasBody()) {
        body = Arrays.copyOfRange(bytes, index + 4, bytes.length);

        if (body.length == 0) {
          throw new MessageException("Empty body in " + messageType + " message");
        }
      } else if (index + 4 != bytes.length) {
        throw new MessageException("Non-empty body in " + messageType + " message");
      }

    } catch (IllegalArgumentException e) {
      throw new MessageException(e.getMessage());
    }
  }

  /**
   * [RECEIVE] Constructs a message directly from a block of bytes given address and port.
   *
   * @param bytes   The entire message as a byte string
   * @param port    The sender's UDP port
   * @param address The sender's IP address
   */
  Message(byte[] bytes, int port, InetAddress address) throws MessageException {
    this(bytes);

    this.port = port;
    this.address = address;
  }

  /**
   * [RECEIVE] Construct a message directly from a received Datagram packet.
   *
   * @param packet The received UDP packet
   */
  Message(DatagramPacket packet) throws MessageException {
    this(packet.getData(), packet.getPort(), packet.getAddress());
  }

  /**
   * [SEND] Construct a message given all fields.
   */
  Message(MessageType type, String version, String fileId, int chunkNo,
          int replication, String[] more, byte[] body) {
    try {
      this.messageType = type;

      validateVersion(version);
      this.version = version;

      validateFileId(fileId);
      this.fileId = fileId;

      validateChunkNo(chunkNo);
      this.chunkNo = chunkNo;

      validateReplicationDegree(replication);
      this.replication = replication;

      this.more = more == null ? new String[0] : more;
      this.body = body;
    } catch (MessageException e) {
      throw new MessageError(e);
    }
  }

  /**
   * [SEND] Construct a message given all fields plus sender id.
   */
  Message(MessageType type, String version, String fileId, int chunkNo,
          int replication, String[] more, byte[] body, String senderId) {
    this(type, version, fileId, chunkNo, replication, more, body);

    try {
      validateSenderId(senderId);
      this.senderId = senderId;
    } catch (MessageException e) {
      throw new MessageError(e);
    }
  }

  /**
   * [SEND] Construct a message given all fields, sender id, destination address and port.
   */
  Message(MessageType type, String version, String fileId, int chunkNo,
          int replication, String[] more, byte[] body, String senderId,
          InetAddress address, int port) throws MessageException {
    this(type, version, fileId, chunkNo, replication, more, body, senderId);

    this.address = address;
    this.port = port;
  }

  /**
   * * Constructor shortcuts
   */

  public static Message PUTCHUNK(String fileId, int chunkNo, int replication, byte[] body) {
    return new Message(MessageType.PUTCHUNK, Protocol.version, fileId, chunkNo,
        replication, null, body);
  }

  public static Message STORED(String fileId, int chunkNo) {
    return new Message(MessageType.STORED, Protocol.version, fileId, chunkNo, 0, null,
        null);
  }

  public static Message GETCHUNK(String fileId, int chunkNo) {
    return new Message(MessageType.GETCHUNK, Protocol.version, fileId, chunkNo, 0, null,
        null);
  }

  public static Message CHUNK(String fileId, int chunkNo, byte[] body) {
    return new Message(MessageType.CHUNK, Protocol.version, fileId, chunkNo, 0, null,
        body);
  }

  public static Message DELETE(String fileId) {
    return new Message(MessageType.DELETE, Protocol.version, fileId, 0, 0, null, null);
  }

  public static Message REMOVED(String fileId, int chunkNo) {
    return new Message(MessageType.REMOVED, Protocol.version, fileId, chunkNo, 0, null,
        null);
  }

  /**
   * * Getters and setters
   */

  public MessageType getType() {
    return messageType;
  }

  public String getVersion() {
    return version;
  }

  public String getFileId() {
    return fileId;
  }

  public int getChunkNo() {
    if (messageType.fields() < 5)
      throw new IllegalStateException("This message type has no chunk number");
    return chunkNo;
  }

  public int getReplication() {
    if (messageType.fields() < 6)
      throw new IllegalStateException("This message type has no replication degree");
    return replication;
  }

  public String[] getExtraHeaders() {
    return more;
  }

  public byte[] getBody() {
    if (!messageType.hasBody())
      throw new IllegalStateException("This message type does not have a body");
    return body;
  }

  public String getSenderId() {
    return senderId;
  }

  public InetAddress getAddress() {
    return address;
  }

  public int getPort() {
    return port;
  }

  void setSenderId(String senderId) {
    try {
      validateSenderId(senderId);
    } catch (MessageException e) {
      throw new MessageError(e);
    }
    this.senderId = senderId;
  }

  void setAddress(InetAddress address) {
    this.address = address;
  }

  void setPort(int port) {
    this.port = port;
  }

  @Override
  public String toString() {
    StringBuilder text = new StringBuilder("Message");
    text.append("\n ").append(headerString());
    text.append("\n message type: ").append(messageType);
    text.append("\n      version: ").append(version);
    text.append("\n       sender: ").append(senderId);
    text.append("\n      file id: ").append(fileId);
    text.append("\n        chunk: ").append(chunkNo);
    text.append("\n  replication: ").append(replication);
    text.append("\n         body: ").append(body != null ? body.length : "null");
    if (address != null && port != 0) {
      text.append("\n to/from: ").append(address.toString()).append(":").append(port);
    }
    text.append("\n");
    return text.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Message message = (Message) o;
    return chunkNo == message.chunkNo &&
        replication == message.replication &&
        messageType == message.messageType &&
        version.equals(message.version) &&
        Objects.equals(senderId, message.senderId) &&
        fileId.equals(message.fileId) &&
        Arrays.equals(more, message.more) &&
        Arrays.equals(body, message.body);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(messageType, version, senderId, fileId, chunkNo, replication);
    result = 31 * result + Arrays.hashCode(more);
    result = 31 * result + Arrays.hashCode(body);
    return result;
  }
}
