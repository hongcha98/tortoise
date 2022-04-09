package io.github.hongcha98.turtles.broker.error;

public class TopicNotExistsException extends TopicException {
    public TopicNotExistsException(String topic) {
        super("topic " + topic + " not exists");
    }
}
