package io.github.hongcha98.turtles.broker.process.topic;

import io.github.hongcha98.turtles.broker.TurtlesBroker;
import io.github.hongcha98.turtles.broker.context.ChannelContext;
import io.github.hongcha98.turtles.broker.process.AbstractProcess;
import io.github.hongcha98.turtles.common.dto.topic.UnSubscriptionMessageReq;
import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

import java.util.Objects;

public class UnSubscriptionProcess extends AbstractProcess {
    public UnSubscriptionProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
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
