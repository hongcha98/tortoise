package io.github.hongcha98.turtles.common.error;

public class TurtlesException extends RuntimeException {

    public TurtlesException() {
    }

    public TurtlesException(String message) {
        super(message);
    }

    public TurtlesException(String message, Throwable cause) {
        super(message, cause);
    }

    public TurtlesException(Throwable cause) {
        super(cause);
    }

    public TurtlesException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
