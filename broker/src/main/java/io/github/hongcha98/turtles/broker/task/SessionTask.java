package io.github.hongcha98.turtles.broker.task;

import io.github.hongcha98.turtles.broker.TurtlesBroker;
import io.github.hongcha98.turtles.broker.context.ChannelContext;
import io.github.hongcha98.turtles.broker.context.ChannelContextManage;
import io.github.hongcha98.turtles.broker.session.SessionManage;
import io.netty.channel.Channel;

import java.util.Map;
import java.util.Set;

public class SessionTask extends AbstractTask {
    public SessionTask(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
    }

    @Override
    public void run() {
        try {
            SessionManage sessionManage = getBroker().getSessionManage();
            ChannelContextManage channelContextManage = getBroker().getChannelContextManage();
            Map<Channel, ChannelContext> allChannelContext = channelContextManage.getAllChannelContext();
            allChannelContext.forEach((channel, channelContext) -> {
                boolean active = channel.isActive();
                String groupName = channelContext.getGroupName();
                if (!active) {
                    channelContextManage.deleteChannelContext(channel);
                }
                boolean subscription = active && groupName != null;
                Set<String> topicNames = channelContext.getTopicNames();
                for (String topicName : topicNames) {
                    if (subscription) {
                        sessionManage.subscription(topicName, groupName, channel);
                    } else {
                        sessionManage.unSubscription(topicName, groupName, channel);
                    }
                }
            });
        } catch (Exception e) {
            log.error("session task error", e);
        }

    }
}
