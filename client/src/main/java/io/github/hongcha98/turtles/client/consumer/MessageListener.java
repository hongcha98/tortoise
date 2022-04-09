package io.github.hongcha98.turtles.client.consumer;

import io.github.hongcha98.turtles.common.dto.message.Message;


public interface MessageListener {
    /**
     * 消费消息
     *
     * @param message 消息
     * @return 是否消费成功
     */
    boolean listener(Message message);
}
