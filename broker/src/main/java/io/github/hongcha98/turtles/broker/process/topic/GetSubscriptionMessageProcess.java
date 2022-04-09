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
        String groupName = channelContext.getGroupName();
        SubscriptionInfoResponse resp = new SubscriptionInfoResponse();
        Map<String, Set<Integer>> topicQueuesIdMap = new HashMap<>();
        channelContext.getTopicNames().stream().forEach(topicName -> {
            Set<Integer> allocate = getBroker().getSessionManage().getAllocate(topicName, groupName, channel);
            topicQueuesIdMap.put(topicName, allocate);
        });
        resp.setTopicQueuesIdMap(topicQueuesIdMap);
        response(channelHandlerContext, message, resp);
    }
}
