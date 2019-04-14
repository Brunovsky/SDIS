package dbs.message;

import dbs.Configuration;
import dbs.Utils;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

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

  private String mainHeaderString() {
    String[] parts = new String[messageType.fields()];

    parts[0] = messageType.toString();
    parts[1] = version;
    parts[2] = senderId;
    parts[3] = fileId;
    if (messageType.fields() >= 5) parts[4] = Integer.toString(chunkNo);
    if (messageType.fields() >= 6) parts[5] = Integer.toString(replication);

    return String.join(" ", parts);
  }

  private String headerString() {
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

  public byte[] makeBytes() {
    byte[] header = getHeaderBytes();

    if (body == null) return header;

    int length = header.length + body.length;
    byte[] bytes = new byte[length];

    System.arraycopy(header, 0, bytes, 0, header.length);
    System.arraycopy(body, 0, bytes, header.length, body.length);

    return bytes;
  }

  public DatagramPacket getPacket(int port, InetAddress address) {
    assert senderId != null;
    byte[] bytes = makeBytes();
    return new DatagramPacket(bytes, bytes.length, address, port);
  }

  public DatagramPacket getPacket(String senderId, int port, InetAddress address) {
    setSenderId(senderId);
    return getPacket(port, address);
  }

  private static void validateMainHeader(String [] parts) throws MessageException {
    if (parts.length == 0) {
      throw new MessageException("Main message header is empty");
    }

    MessageType type = MessageType.from(parts[0]);  // can throw too

    if (parts.length != type.fields()) {
      throw new MessageException("Incomplete message header for " + parts[0]
          + " message: expected " + type.fields() + " fields, got " + parts.length + ".");
    }
  }

  public static void validateVersion(String version) throws MessageException {
    if (!Utils.validVersion(version)) {
      throw new MessageException("Invalid protocol version: " + version);
    }
  }

  private static void validateSenderId(String senderId) throws MessageException {
    if (!Utils.validSenderId(senderId)) {
      throw new MessageException("Invalid sender id: " + senderId);
    }
  }

  private static void validateFileId(String fileId) throws MessageException {
    if (!Utils.validFileId(fileId)) {
      throw new MessageException("Invalid file hash: " + fileId);
    }
  }

  private static void validateChunkNo(String chunkNo) throws MessageException {
    if (!Utils.validChunkNo(chunkNo)) {
      throw new MessageException("Invalid chunkNo: " + chunkNo);
    }
  }

  private static void validateChunkNo(int chunkNo) throws MessageException {
    if (!Utils.validChunkNo(chunkNo)) {
      throw new MessageException("Invalid chunkNo: " + chunkNo);
    }
  }

  private static void validateReplicationDegree(String replication)
      throws MessageException {
    if (!Utils.validReplicationDegree(replication)) {
      throw new MessageException("Invalid replication degree: " + replication);
    }
  }

  private static void validateReplicationDegree(int replication) throws MessageException {
    if (!Utils.validReplicationDegree(replication)) {
      throw new MessageException("Replication degree should be 1..9: " + replication);
    }
  }

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
  private void parseHeaders(String [] headers) throws MessageException {
    if (headers.length == 0) {
      throw new MessageException("Message has no headers");
    }

    parseMainHeader(headers[0]);
    more = Arrays.copyOfRange(headers, 1, headers.length);
  }

  /**
   * Parse the message byte array.
   *
   * @param bytes The received message's byte array, properly trimmed.
   * @throws MessageException If there is any problem with the message format whatsoever.
   */
  private void parse(byte [] bytes, int length) throws MessageException {
    try {
      // Find index of first occurrence of \r\n\r\n
      // TODO: use a BufferedStream
      int index = new String(bytes, 0, length, UTF_8).indexOf("\r\n\r\n");
      if (index < 0) {
        throw new MessageException("Invalid Message byte array: no header separator");
      }

      // Headers up to index, body after index
      byte[] headersBytes = Arrays.copyOfRange(bytes, 0, index);
      String[] headers = new String(headersBytes).split("\r\n");

      parseHeaders(headers);

      if (messageType.hasBody()) {
        body = Arrays.copyOfRange(bytes, index + 4, length);
      } else if (index + 4 != length) {
        throw new MessageException("Non-empty body in " + messageType + length);
      }

    } catch (IllegalArgumentException e) {
      throw new MessageException(e.getMessage());
    }
  }

  /**
   * [RECEIVE] Constructs a message directly from a block of bytes, properly trimmed
   */
  public Message(byte [] bytes) throws MessageException {
    parse(bytes, bytes.length);
  }

  /**
   * [RECEIVE] Construct a message directly from a received Datagram packet.
   *
   * @param packet The received UDP packet
   */
  public Message(DatagramPacket packet) throws MessageException {
    parse(packet.getData(), packet.getLength());

    this.port = packet.getPort();
    this.address = packet.getAddress();
  }

  /**
   * [SEND] Construct a message given all fields.
   */
  private Message(MessageType type, String version,
                  String fileId, int chunkNo, int replication,
                  String[] more,
                  byte[] body) throws MessageError {
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
   * Construct a PUTCHUNK message. Required camps: fileId, chunkNo, replication, and body.
   *
   * @param fileId      This chunk's file id
   * @param version     The protocol's version
   * @param chunkNo     This chunk's number
   * @param replication This chunk's desired replication degree
   * @param body        The chunk content
   * @return The constructed Message
   * @throws MessageError   If any of the fields has a protocol-prohibited value
   * @throws AssertionError If any of the fields has an invalid value
   */
  public static Message PUTCHUNK(String fileId, String version,
                                 int chunkNo, int replication, byte [] body) {
    return new Message(MessageType.PUTCHUNK, version, fileId, chunkNo,
        replication, null, body);
  }

  public static Message PUTCHUNK(String fileId, int chunkNo, int replication,
                                 byte [] body) {
    return PUTCHUNK(fileId, Configuration.version, chunkNo, replication, body);
  }

  /**
   * Construct a STORED message. Required camps: fileId and chunkNo.
   *
   * @param fileId  The stored chunk's file id
   * @param version The protocol's version
   * @param chunkNo The stored chunk's number
   * @return The constructed Message
   * @throws MessageError   If any of the fields has a protocol-prohibited value
   * @throws AssertionError If any of the fields has an invalid value
   */
  public static Message STORED(String fileId, String version,
                               int chunkNo) {
    return new Message(MessageType.STORED, version, fileId, chunkNo, 0, null,
        null);
  }

  public static Message STORED(String fileId, int chunkNo) {
    return STORED(fileId, Configuration.version, chunkNo);
  }

  /**
   * Construct a GETCHUNK message. Required camps: fileId and chunkNo.
   *
   * @param fileId  The desired chunk's file id
   * @param version The protocol's version
   * @param chunkNo The desired chunk's number
   * @return The constructed Message
   * @throws MessageError   If any of the fields has a protocol-prohibited value
   * @throws AssertionError If any of the fields has an invalid value
   */
  public static Message GETCHUNK(String fileId, String version,
                                 int chunkNo) {
    return new Message(MessageType.GETCHUNK, version, fileId, chunkNo, 0, null,
        null);
  }

  public static Message GETCHUNK(String fileId, int chunkNo) {
    return GETCHUNK(fileId, Configuration.version, chunkNo);
  }

  /**
   * Construct a CHUNK message. Required camps: fileId, chunkNo and body.
   *
   * @param fileId  This chunk's file id
   * @param version The protocol's version
   * @param chunkNo This chunk's number
   * @param body    The chunk content
   * @return The constructed Message
   * @throws MessageError   If any of the fields has a protocol-prohibited value
   * @throws AssertionError If any of the fields has an invalid value
   */
  public static Message CHUNK(String fileId, String version,
                              int chunkNo, byte [] body) {
    return new Message(MessageType.CHUNK, version, fileId, chunkNo, 0, null,
        body);
  }

  public static Message CHUNK(String fileId, int chunkNo,
                              byte [] body) {
    return CHUNK(fileId, Configuration.version, chunkNo, body);
  }

  /**
   * Construct a DELETE message. Required camps: fileId.
   *
   * @param fileId  The id of the file to be deleted
   * @param version The protocol's version
   * @return The constructed Message
   * @throws MessageError   If any of the fields has a protocol-prohibited value
   * @throws AssertionError If any of the fields has an invalid value
   */
  public static Message DELETE(String fileId, String version) {
    return new Message(MessageType.DELETE, version, fileId, 0, 0, null, null);
  }

  public static Message DELETE(String fileId) {
    return DELETE(fileId, Configuration.version);
  }

  /**
   * Construct a DELETED message. Required camps: fileId.
   *
   * @param fileId  The id of the file whose chunks were deleted
   * @param version The protocol's version
   * @return The constructed Message
   * @throws MessageError   If any of the fields has a protocol-prohibited value
   * @throws AssertionError If any of the fields has an invalid value
   */
  public static Message DELETED(String fileId, String version) {
    return new Message(MessageType.DELETED, version, fileId, 0, 0, null, null);
  }

  public static Message DELETED(String fileId) {
    return DELETED(fileId, Configuration.version);
  }

  /**
   * Construct a REMOVED message. Required camps: fileId and chunkNo
   *
   * @param fileId  The removed chunk's file id
   * @param version The protocol's version
   * @param chunkNo The removed chunk's number
   * @return The constructed Message
   * @throws MessageError   If any of the fields has a protocol-prohibited value
   * @throws AssertionError If any of the fields has an invalid value
   */
  public static Message REMOVED(String fileId, String version,
                                int chunkNo) {
    return new Message(MessageType.REMOVED, version, fileId, chunkNo, 0, null,
        null);
  }

  public static Message REMOVED(String fileId, int chunkNo) {
    return REMOVED(fileId, Configuration.version, chunkNo);
  }

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

  public void setSenderId(String senderId) {
    try {
      validateSenderId(senderId);
    } catch (MessageException e) {
      throw new MessageError(e);
    }
    this.senderId = senderId;
  }

  public void setAddress(InetAddress address) {
    this.address = address;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String shortText() {
    String base = "";
    switch (messageType) {
      case PUTCHUNK:
        base = "PUTCHUNK(" + fileId.substring(0, 10) + ',' + chunkNo + ')';
        break;
      case CHUNK:
        base = "CHUNK(" + fileId.substring(0, 10) + ',' + chunkNo + ')';
        break;
      case STORED:
        base = "STORED(" + fileId.substring(0, 10) + ',' + chunkNo + ')';
        break;
      case DELETE:
        base = "DELETE(" + fileId.substring(0, 10) + ')';
        break;
      case REMOVED:
        base = "REMOVED(" + fileId.substring(0, 10) + ',' + chunkNo + ')';
        break;
      case GETCHUNK:
        base = "GETCHUNK(" + fileId.substring(0, 10) + ',' + chunkNo + ')';
        break;
      case DELETED:
        base = "PUTCHUNK(" + fileId.substring(0, 10) + ')';
        break;
    }
    return base;
  }

  public String shortFrom() {
    return shortText() + " from " + senderId;
  }

  @Override
  public String toString() {
    StringBuilder text = new StringBuilder("Message");
    text.append("\n ").append(headerString());
    text.append("\n message type: ").append(messageType).append(" ").append(version);
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
    return chunkNo == message.chunkNo && replication == message.replication
        && messageType == message.messageType && version.equals(message.version)
        && Objects.equals(senderId, message.senderId) && fileId.equals(message.fileId)
        && Arrays.equals(more, message.more) && Arrays.equals(body, message.body);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(messageType, version, senderId, fileId, chunkNo,
        replication);
    result = 31 * result + Arrays.hashCode(more);
    result = 31 * result + Arrays.hashCode(body);
    return result;
  }
}

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