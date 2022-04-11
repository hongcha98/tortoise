package io.github.hongcha98.tortoise.broker.process.session;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.github.hongcha98.tortoise.broker.context.ChannelContext;
import io.github.hongcha98.tortoise.broker.process.AbstractProcess;
import io.github.hongcha98.tortoise.broker.TortoiseBroker;
import io.github.hongcha98.tortoise.common.dto.session.request.SubscriptionRequest;
import io.netty.channel.ChannelHandlerContext;

public class SubscriptionProcess extends AbstractProcess {
    public SubscriptionProcess(TortoiseBroker tortoiseBroker) {
        super(tortoiseBroker);
    }

    @Override
    public void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        SubscriptionRequest subscriptionRequest = ProtocolUtils.decode(message, SubscriptionRequest.class);
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel());
        channelContext.setGroup(subscriptionRequest.getGroup());
        channelContext.setTopics(subscriptionRequest.getTopics());
        response(channelHandlerContext, message, true);
    }
}
