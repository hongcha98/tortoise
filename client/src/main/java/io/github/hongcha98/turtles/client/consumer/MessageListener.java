package io.github.hongcha98.turtles.client.consumer;

import io.github.hongcha98.turtles.common.dto.message.Message;


public interface MessageListener {
    void listener(Message message);
}
