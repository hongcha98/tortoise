package com.hongcha.turtles.broker.process.topic;

import com.hongcha.turtles.broker.TurtlesBroker;
import com.hongcha.turtles.broker.context.ChannelContext;
import com.hongcha.turtles.broker.process.AbstractProcess;
import com.hongcha.turtles.common.dto.topic.GetSubscriptionMessageResp;
import com.hongcha.remote.common.Message;
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
        GetSubscriptionMessageResp resp = new GetSubscriptionMessageResp();
        Map<String, Set<Integer>> topicQueuesIdMap = new HashMap<>();
        channelContext.getTopicNames().stream().forEach(topicName -> {
            Set<Integer> allocate = getBroker().getSessionManage().getAllocate(topicName, groupName, channel);
            topicQueuesIdMap.put(topicName, allocate);
        });
        resp.setTopicQueuesIdMap(topicQueuesIdMap);
        response(channelHandlerContext, message, resp);
    }
}
