package io.github.hongcha98.tortoise.common.error;

public class TopicNotExistsException extends TopicException {
    public TopicNotExistsException(String topic) {
        super("topic " + topic + " not exists");
    }
}
