package io.github.hongcha98.tortoise.broker.process.topic;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.github.hongcha98.tortoise.broker.TortoiseBroker;
import io.github.hongcha98.tortoise.broker.process.AbstractProcess;
import io.github.hongcha98.tortoise.broker.topic.TopicManage;
import io.github.hongcha98.tortoise.common.dto.topic.request.TopicCreateRequest;
import io.netty.channel.ChannelHandlerContext;

public class TopicCreateProcess extends AbstractProcess {
    public TopicCreateProcess(TortoiseBroker tortoiseBroker) {
        super(tortoiseBroker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        TopicCreateRequest topicCreateRequest = ProtocolUtils.decode(message, TopicCreateRequest.class);
        String topic = topicCreateRequest.getTopic();
        int queueNumber = topicCreateRequest.getQueueNumber();
        TopicManage topicManage = getBroker().getTopicManage();
        if (topicManage.exists(topic)) {
            responseException(channelHandlerContext, message, new IllegalStateException("topic " + topic + " exists"));
        } else {
            boolean flag = false;
            try {
                topicManage.addTopic(topic, queueNumber);
                flag = true;
            } catch (Exception e) {
                LOG.error("topic create error", e);
            }
            response(channelHandlerContext, message, flag);
        }
    }

}
