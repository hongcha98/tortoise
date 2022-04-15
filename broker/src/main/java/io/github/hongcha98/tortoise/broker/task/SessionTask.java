package io.github.hongcha98.tortoise.broker.task;

import io.github.hongcha98.tortoise.broker.TortoiseBroker;
import io.github.hongcha98.tortoise.broker.context.ChannelContext;
import io.github.hongcha98.tortoise.broker.context.ChannelContextManage;
import io.github.hongcha98.tortoise.broker.session.SessionManage;
import io.netty.channel.Channel;

import java.util.Map;
import java.util.Set;

public class SessionTask extends AbstractTask {
    public SessionTask(TortoiseBroker tortoiseBroker) {
        super(tortoiseBroker);
    }

    @Override
    public void run() {
        try {
            SessionManage sessionManage = getBroker().getSessionManage();
            ChannelContextManage channelContextManage = getBroker().getChannelContextManage();
            Map<Channel, ChannelContext> allChannelContext = channelContextManage.getAllChannelContext();
            allChannelContext.forEach((channel, channelContext) -> {
                boolean active = channel.isActive();
                String group = channelContext.getGroup();
                if (!active) {
                    channelContextManage.deleteChannelContext(channel);
                }
                boolean subscription = active && group != null;
                Set<String> topics = channelContext.getTopics();
                for (String topic : topics) {
                    if (subscription) {
                        sessionManage.subscription(topic, group, channel);
                    } else {
                        sessionManage.unSubscription(topic, group, channel);
                    }
                }
            });
        } catch (Exception e) {
            LOG.error("session task error", e);
        }

    }
}
