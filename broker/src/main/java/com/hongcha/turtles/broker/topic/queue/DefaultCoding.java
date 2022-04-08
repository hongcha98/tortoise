package com.hongcha.turtles.broker.topic.queue;

import com.hongcha.turtles.broker.constant.Constant;
import com.hongcha.turtles.common.dto.message.Message;
import com.hongcha.turtles.common.dto.message.MessageInfo;
import com.hongcha.remote.common.spi.SpiDescribe;
import com.hongcha.remote.common.spi.SpiLoader;
import com.hongcha.remote.protocol.Protocol;

import java.nio.ByteBuffer;

/**
 * 默认编解码
 */
@SpiDescribe(name = "turtles")
public class DefaultCoding implements Coding {
    @Override
    public byte[] encode(Message message) {
        byte[] bytes = SpiLoader.load(Protocol.class, Constant.PROTOCOL_CODE).encode(message);
        ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length + 4);
        // 先记录数据的长度,反序列化要使用到
        byteBuffer.putInt(bytes.length);
        byteBuffer.put(bytes);
        return byteBuffer.array();
    }

    @Override
    public MessageInfo decode(ByteBuffer byteBuffer, int offset) {
        /**
         * 获取旧的position，后面设置回去
         */
        int oldPosition = byteBuffer.position();
        try {
            byteBuffer.position(offset);
            /**
             * 读取消息长度
             */
            int length = byteBuffer.getInt(offset);
            if (length == 0) return null;
            byteBuffer.position(offset + 4);
            byte[] bytes = new byte[length];
            byteBuffer.get(bytes);
            Message message = SpiLoader.load(Protocol.class, Constant.PROTOCOL_CODE).decode(bytes, Message.class);
            int nextOffset = offset + 4 + length;
            return new MessageInfo(message, offset, nextOffset);
        } finally {
            byteBuffer.position(oldPosition);
        }
    }
}
