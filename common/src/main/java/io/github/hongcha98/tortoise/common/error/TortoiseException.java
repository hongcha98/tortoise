package io.github.hongcha98.tortoise.common.error;

public class TortoiseException extends RuntimeException {

    public TortoiseException() {
    }

    public TortoiseException(String message) {
        super(message);
    }

    public TortoiseException(String message, Throwable cause) {
        super(message, cause);
    }

    public TortoiseException(Throwable cause) {
        super(cause);
    }

    public TortoiseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
