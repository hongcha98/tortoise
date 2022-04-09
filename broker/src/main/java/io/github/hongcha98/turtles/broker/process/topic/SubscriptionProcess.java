package io.github.hongcha98.turtles.broker.process.topic;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.github.hongcha98.turtles.broker.TurtlesBroker;
import io.github.hongcha98.turtles.broker.context.ChannelContext;
import io.github.hongcha98.turtles.broker.process.AbstractProcess;
import io.github.hongcha98.turtles.common.dto.topic.SubscriptionRequest;
import io.netty.channel.ChannelHandlerContext;

public class SubscriptionProcess extends AbstractProcess {
    public SubscriptionProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
    }

    @Override
    public void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        SubscriptionRequest subscriptionRequest = ProtocolUtils.decode(message, SubscriptionRequest.class);
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel());
        channelContext.setGroupName(subscriptionRequest.getGroupName());
        channelContext.setTopicNames(subscriptionRequest.getTopicNames());
        response(channelHandlerContext, message, true);
    }
}
