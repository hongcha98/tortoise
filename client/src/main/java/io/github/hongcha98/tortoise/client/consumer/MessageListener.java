package io.github.hongcha98.tortoise.client.consumer;

import io.github.hongcha98.tortoise.common.dto.message.MessageEntry;


public interface MessageListener {
    /**
     * 消费消息
     *
     * @param messageEntry 消息
     * @return 是否消费成功
     */
    boolean listener(MessageEntry messageEntry);
}
