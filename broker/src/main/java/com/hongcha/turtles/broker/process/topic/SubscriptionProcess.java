package com.hongcha.turtles.broker.process.topic;

import com.hongcha.turtles.broker.TurtlesBroker;
import com.hongcha.turtles.broker.context.ChannelContext;
import com.hongcha.turtles.broker.process.AbstractProcess;
import com.hongcha.turtles.common.dto.topic.SubscriptionMessageReq;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

public class SubscriptionProcess extends AbstractProcess {
    public SubscriptionProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
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
