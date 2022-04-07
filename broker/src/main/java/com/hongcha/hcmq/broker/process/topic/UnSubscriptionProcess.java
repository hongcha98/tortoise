package com.hongcha.hcmq.broker.process.topic;

import com.hongcha.hcmq.broker.Broker;
import com.hongcha.hcmq.broker.context.ChannelContext;
import com.hongcha.hcmq.broker.process.AbstractProcess;
import com.hongcha.hcmq.common.dto.topic.UnSubscriptionMessageReq;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

import java.util.Objects;

public class UnSubscriptionProcess extends AbstractProcess {
    public UnSubscriptionProcess(Broker broker) {
        super(broker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        UnSubscriptionMessageReq unSubscriptionMessageReq = ProtocolUtils.decode(message, UnSubscriptionMessageReq.class);
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel());
        if (Objects.equals(channelContext.getGroupName(), unSubscriptionMessageReq.getGroupName())) {
            channelContext.getTopicNames().removeAll(unSubscriptionMessageReq.getTopicNames());
        }
        response(channelHandlerContext, message, true);
    }
}
