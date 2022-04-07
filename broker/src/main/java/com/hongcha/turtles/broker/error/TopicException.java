package com.hongcha.turtles.broker.error;

public abstract class TopicException extends TurtlesException {
    public TopicException(String msg) {
        super(msg);
    }
}
