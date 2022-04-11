package io.github.hongcha98.tortoise.broker.process.session;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.github.hongcha98.tortoise.broker.context.ChannelContext;
import io.github.hongcha98.tortoise.broker.process.AbstractProcess;
import io.github.hongcha98.tortoise.broker.TortoiseBroker;
import io.github.hongcha98.tortoise.common.dto.session.request.UnSubscriptionRequest;
import io.netty.channel.ChannelHandlerContext;

import java.util.Objects;

public class UnSubscriptionProcess extends AbstractProcess {
    public UnSubscriptionProcess(TortoiseBroker tortoiseBroker) {
        super(tortoiseBroker);
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
