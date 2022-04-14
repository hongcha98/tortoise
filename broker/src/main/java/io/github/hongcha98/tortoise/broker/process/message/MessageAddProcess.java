package io.github.hongcha98.tortoise.broker.process.message;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.github.hongcha98.tortoise.broker.TortoiseBroker;
import io.github.hongcha98.tortoise.broker.constant.Constant;
import io.github.hongcha98.tortoise.broker.process.AbstractProcess;
import io.github.hongcha98.tortoise.broker.topic.Topic;
import io.github.hongcha98.tortoise.common.dto.message.MessageEntry;
import io.github.hongcha98.tortoise.common.dto.message.request.MessageAddRequest;
import io.github.hongcha98.tortoise.common.error.TopicException;
import io.netty.channel.ChannelHandlerContext;

public class MessageAddProcess extends AbstractProcess {
    public MessageAddProcess(TortoiseBroker tortoiseBroker) {
        super(tortoiseBroker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        MessageAddRequest messageAddRequest = ProtocolUtils.decode(message, MessageAddRequest.class);
        Topic topic;
        try {
            topic = getBroker().getTopicManage().getTopic(messageAddRequest.getTopic());
        } catch (TopicException e) {
            responseException(channelHandlerContext, message, e);
            return;
        }
        MessageEntry messageEntry = new MessageEntry();
        messageEntry.setHeader(messageAddRequest.getHeader());
        messageEntry.setBody(messageAddRequest.getBody());
        // 如果是延时消息
        int offset;
        int delayLevel = messageAddRequest.getDelayLevel();
        if (delayLevel != 0) {
            topic = getBroker().getTopicManage().getTopic(Constant.DELAY_TOPIC);
            messageEntry.getHeader().put(Constant.DELAY_HEADER_TOPIC, messageAddRequest.getTopic());
            offset = topic.addMessage(delayLevel - 1, messageEntry, messageAddRequest.isBrush());
        } else {
            offset = topic.addMessage(messageEntry, messageAddRequest.isBrush());
        }
        LOG.info("message topic : {} , id :{} ,  add success , offset : {}", topic.getName(), messageEntry.getId(), offset);
        response(channelHandlerContext, message, messageEntry.getId());
    }
}
