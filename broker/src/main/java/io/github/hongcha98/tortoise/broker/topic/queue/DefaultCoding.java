package io.github.hongcha98.tortoise.broker.topic.queue;

import io.github.hongcha98.remote.common.spi.SpiDescribe;
import io.github.hongcha98.remote.common.spi.SpiLoader;
import io.github.hongcha98.remote.protocol.Protocol;
import io.github.hongcha98.tortoise.broker.constant.Constant;
import io.github.hongcha98.tortoise.common.dto.message.Message;
import io.github.hongcha98.tortoise.common.dto.message.MessageInfo;

import java.nio.ByteBuffer;

/**
 * 默认编解码
 */
@SpiDescribe(name = "tortoise")
public class DefaultCoding implements Coding {
    private final Protocol protocol;

    public DefaultCoding(Protocol protocol) {
        this.protocol = protocol;
    }


    @Override
    public byte[] encode(Message message) {
        byte[] bytes = SpiLoader.load(Protocol.class, Constant.PROTOCOL_CODE).encode(message);
        ByteBuffer byteBuffer = ByteBuffer.allocate(Constant.MESSAGE_METADATA_LENGTH + bytes.length);
        byteBuffer.putInt(bytes.length);
        byteBuffer.put((byte) 0);
        byteBuffer.put(bytes);
        return byteBuffer.array();
    }

    @Override
    public MessageInfo decode(ByteBuffer byteBuffer, int offset, boolean consumer) {
        //获取旧的position，后面设置回去
        int oldPosition = byteBuffer.position();
        try {
            byteBuffer.position(offset);
            // 读取消息长度
            int length = byteBuffer.getInt(offset);
            if (length == 0) return null;
            // 读取消费次数
            byte consumptionTimes = byteBuffer.get(offset + Constant.MESSAGE_LENGTH);
            if (consumer) {
                consumptionTimes += 1;
                byteBuffer.put(offset + Constant.MESSAGE_LENGTH, consumptionTimes);
            }
            byteBuffer.position(offset + Constant.MESSAGE_METADATA_LENGTH);
            byte[] bytes = new byte[length];
            byteBuffer.get(bytes);
            Message message = protocol.decode(bytes, Message.class);
            int nextOffset = offset + Constant.MESSAGE_METADATA_LENGTH + length;
            return new MessageInfo(message, consumptionTimes, offset, nextOffset);
        } finally {
            byteBuffer.position(oldPosition);
        }
    }
}
