package com.hongcha.turtles.client.consumer;

import com.hongcha.turtles.common.dto.message.Message;


public interface MessageListener {
    void listener(Message message);
}
