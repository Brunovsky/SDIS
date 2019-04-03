package dbs.message;

import org.jetbrains.annotations.NotNull;

public class MessageError extends Error {
    static final long serialVersionUID = 37L;

    public MessageError(@NotNull String s) {
        super(s);
    }

    public MessageError(@NotNull String s, @NotNull Throwable cause) {
        super(s, cause);
    }

    public MessageError(@NotNull MessageException e) {
        super(e.getMessage(), e.getCause());
    }
}