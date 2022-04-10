package io.github.hongcha98.turtles.broker.process.topic;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.turtles.broker.TurtlesBroker;
import io.github.hongcha98.turtles.broker.context.ChannelContext;
import io.github.hongcha98.turtles.broker.process.AbstractProcess;
import io.github.hongcha98.turtles.common.dto.topic.SubscriptionInfoResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GetSubscriptionMessageProcess extends AbstractProcess {
    public GetSubscriptionMessageProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        Channel channel = channelHandlerContext.channel();
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channel);
        String group = channelContext.getGroup();
        SubscriptionInfoResponse resp = new SubscriptionInfoResponse();
        Map<String, Set<Integer>> topicQueuesIdMap = new HashMap<>();
        channelContext.getTopics().stream().forEach(topic -> {
            Set<Integer> allocate = getBroker().getSessionManage().getAllocate(topic, group, channel);
            topicQueuesIdMap.put(topic, allocate);
        });
        resp.setTopicQueuesIdMap(topicQueuesIdMap);
        response(channelHandlerContext, message, resp);
    }
}
