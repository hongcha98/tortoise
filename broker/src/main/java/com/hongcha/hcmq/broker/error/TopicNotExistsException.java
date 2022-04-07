package com.hongcha.hcmq.broker.error;

public class TopicNotExistsException extends TopicException {
    public TopicNotExistsException(String topic) {
        super("topic " + topic + " not exists");
    }
}
