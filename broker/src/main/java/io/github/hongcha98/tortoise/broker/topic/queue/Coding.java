package io.github.hongcha98.tortoise.broker.topic.queue;

import io.github.hongcha98.tortoise.common.dto.message.Message;
import io.github.hongcha98.tortoise.common.dto.message.MessageInfo;

import java.nio.ByteBuffer;

/**
 * 消息编码解码
 */
public interface Coding {
    /**
     * 编码
     *
     * @param message 消息
     * @return
     */
    byte[] encode(Message message);

    /**
     * 解码,并获取下一条消息的offset,如果offset没有则返回null
     *
     * @param byteBuffer
     * @param offset
     * @param consumer
     * @return
     */
    MessageInfo decode(ByteBuffer byteBuffer, int offset, boolean consumer);
}
