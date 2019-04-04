package dbs.message;

import org.jetbrains.annotations.NotNull;

public class MessageException extends Exception {
  static final long serialVersionUID = 73L;

  public MessageException(@NotNull String s) {
    super(s);
  }

  public MessageException(@NotNull String s, @NotNull Throwable cause) {
    super(s, cause);
  }
}