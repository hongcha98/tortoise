package com.hongcha.hcmq.broker.process.topic;

import com.hongcha.hcmq.broker.Broker;
import com.hongcha.hcmq.broker.context.ChannelContext;
import com.hongcha.hcmq.broker.process.AbstractProcess;
import com.hongcha.hcmq.common.dto.topic.SubscriptionMessageReq;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

public class SubscriptionProcess extends AbstractProcess {
    public SubscriptionProcess(Broker broker) {
        super(broker);
    }

    @Override
    public void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        SubscriptionMessageReq subscriptionMessageReq = ProtocolUtils.decode(message, SubscriptionMessageReq.class);
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel());
        channelContext.setGroupName(subscriptionMessageReq.getGroupName());
        channelContext.setTopicNames(subscriptionMessageReq.getTopicNames());
        response(channelHandlerContext, message, true);
    }
}
