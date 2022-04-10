package io.github.hongcha98.turtles.broker.process.topic;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.github.hongcha98.turtles.broker.TurtlesBroker;
import io.github.hongcha98.turtles.broker.context.ChannelContext;
import io.github.hongcha98.turtles.broker.process.AbstractProcess;
import io.github.hongcha98.turtles.common.dto.topic.UnSubscriptionRequest;
import io.netty.channel.ChannelHandlerContext;

import java.util.Objects;

public class UnSubscriptionProcess extends AbstractProcess {
    public UnSubscriptionProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        UnSubscriptionRequest unSubscriptionRequest = ProtocolUtils.decode(message, UnSubscriptionRequest.class);
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel());
        if (Objects.equals(channelContext.getGroup(), unSubscriptionRequest.getGroup())) {
            channelContext.getTopics().removeAll(unSubscriptionRequest.getTopics());
        }
        response(channelHandlerContext, message, true);
    }
}
