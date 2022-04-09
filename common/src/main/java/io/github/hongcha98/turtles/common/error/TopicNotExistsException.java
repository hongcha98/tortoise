package io.github.hongcha98.turtles.common.error;

public class TopicNotExistsException extends TopicException {
    public TopicNotExistsException(String topic) {
        super("topic " + topic + " not exists");
    }
}
