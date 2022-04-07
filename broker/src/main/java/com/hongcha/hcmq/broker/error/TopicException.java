package com.hongcha.hcmq.broker.error;

public abstract class TopicException extends HcmqException {
    public TopicException(String msg) {
        super(msg);
    }
}
