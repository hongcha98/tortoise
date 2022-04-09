package io.github.hongcha98.turtles.broker.error;

public abstract class TopicException extends TurtlesException {
    public TopicException(String msg) {
        super(msg);
    }
}
