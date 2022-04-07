package com.hongcha.hcmq.broker.process.topic;

import com.hongcha.hcmq.broker.Broker;
import com.hongcha.hcmq.broker.process.AbstractProcess;
import com.hongcha.hcmq.broker.topic.TopicManage;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

public class TopicDeleteProcess extends AbstractProcess {
    public TopicDeleteProcess(Broker broker) {
        super(broker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        String topicName = ProtocolUtils.decode(message, String.class);
        TopicManage topicManage = getBroker().getTopicManage();
        topicManage.deleteTopic(topicName);
        response(channelHandlerContext, message, true);
    }

}
