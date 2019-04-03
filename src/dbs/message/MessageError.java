package dbs.message;

public class MessageError extends Error {
  static final long serialVersionUID = 37L;

  public MessageError(String s) {
    super(s);
  }

  public MessageError(String s, Throwable cause) {
    super(s, cause);
  }

  public MessageError(MessageException e) {
    super(e.getMessage(), e.getCause());
  }
}
