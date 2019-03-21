package dbs;

public enum MessageType {
  PUTCHUNK("PUTCHUNK"),
  STORED("STORED"),
  GETCHUNK("GETCHUNK"),
  CHUNK("CHUNK"),
  DELETE("DELETE"),
  REMOVED("REMOVED");

  String str;

  MessageType(String s) {
    this.str = s;
  }

  public static MessageType from(String s) throws MessageException {
    switch (s) {
    case "PUTCHUNK":
      return PUTCHUNK;
    case "STORED":
      return STORED;
    case "GETCHUNK":
      return GETCHUNK;
    case "CHUNK":
      return CHUNK;
    case "DELETE":
      return DELETE;
    case "REMOVED":
      return REMOVED;
    default:
      throw new MessageException("Unrecognized message type: " + s);
    }
  }

  public int fields() {
    switch (this) {
    case PUTCHUNK:
      return 6;
    case STORED:
    case GETCHUNK:
    case CHUNK:
    case REMOVED:
      return 5;
    case DELETE:
      return 4;
    default:
      throw new IllegalStateException("Invalid message type state for fields() call");
    }
  }

  public boolean hasBody() {
    switch (this) {
    case PUTCHUNK:
    case CHUNK:
      return true;
    default:
      return false;
    }
  }

  @Override
  public String toString() {
    return str;
  }
}

// PUTCHUNK <Version> <SenderId> <FileId> <ChunkNo> <ReplicationDeg> . <Body>
// STORED   <Version> <SenderId> <FileId> <ChunkNo> .
// GETCHUNK <Version> <SenderId> <FileId> <ChunkNo> .
// CHUNK    <Version> <SenderId> <FileId> <ChunkNo> . <Body>
// DELETE   <Version> <SenderId> <FileId> .
// REMOVED  <Version> <SenderId> <FileId> <ChunkNo> .
