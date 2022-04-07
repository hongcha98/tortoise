package com.hongcha.hcmq.broker.error;

public class HcmqException extends RuntimeException {

    public HcmqException() {
    }

    public HcmqException(String message) {
        super(message);
    }

    public HcmqException(String message, Throwable cause) {
        super(message, cause);
    }

    public HcmqException(Throwable cause) {
        super(cause);
    }

    public HcmqException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
