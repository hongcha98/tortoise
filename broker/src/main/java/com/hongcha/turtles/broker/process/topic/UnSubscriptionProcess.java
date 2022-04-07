package com.hongcha.turtles.broker.process.topic;

import com.hongcha.turtles.broker.TurtlesBroker;
import com.hongcha.turtles.broker.context.ChannelContext;
import com.hongcha.turtles.broker.process.AbstractProcess;
import com.hongcha.turtles.common.dto.topic.UnSubscriptionMessageReq;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
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
