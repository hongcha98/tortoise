package com.hongcha.hcmq.broker.process.topic;

import com.hongcha.hcmq.broker.Broker;
import com.hongcha.hcmq.broker.process.AbstractProcess;
import com.hongcha.hcmq.broker.topic.TopicManage;
import com.hongcha.hcmq.common.dto.topic.TopicCreateMessageReq;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

public class TopicCreateProcess extends AbstractProcess {
    public TopicCreateProcess(Broker broker) {
        super(broker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        TopicCreateMessageReq topicCreateMessageReq = ProtocolUtils.decode(message, TopicCreateMessageReq.class);
        String topicName = topicCreateMessageReq.getTopicName();
        int queueNumber = topicCreateMessageReq.getQueueNumber();
        TopicManage topicManage = getBroker().getTopicManage();
        if (topicManage.exists(topicName)) {
            responseException(channelHandlerContext, message, new IllegalStateException("topic " + topicName + " exists"));
        } else {
            boolean flag = false;
            try {
                topicManage.addTopic(topicName, queueNumber);
                flag = true;
            } catch (Exception e) {
                log.error("topic create error", e);
            }
            response(channelHandlerContext, message, flag);
        }
    }

}
