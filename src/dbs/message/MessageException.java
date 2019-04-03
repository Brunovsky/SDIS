package dbs.message;

public class MessageException extends Exception {
  static final long serialVersionUID = 73L;

  public MessageException(String s) {
    super(s);
  }

  public MessageException(String s, Throwable cause) {
    super(s, cause);
  }
}
