package com.hongcha.hcmq.broker.process.topic;

import com.hongcha.hcmq.broker.Broker;
import com.hongcha.hcmq.broker.context.ChannelContext;
import com.hongcha.hcmq.broker.process.AbstractProcess;
import com.hongcha.hcmq.common.dto.topic.GetSubscriptionMessageResp;
import com.hongcha.remote.common.Message;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GetSubscriptionMessageProcess extends AbstractProcess {
    public GetSubscriptionMessageProcess(Broker broker) {
        super(broker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        Channel channel = channelHandlerContext.channel();
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channel);
        String groupName = channelContext.getGroupName();
        GetSubscriptionMessageResp resp = new GetSubscriptionMessageResp();
        Map<String, Set<Integer>> topicQueuesIdMap = new HashMap<>();
        channelContext.getTopicNames().stream().forEach(topicName -> {
            Set<Integer> allocate = getBroker().getSubscriptionManage().getAllocate(topicName, groupName, channel);
            topicQueuesIdMap.put(topicName, allocate);
        });
        resp.setTopicQueuesIdMap(topicQueuesIdMap);
        response(channelHandlerContext, message, resp);
    }
}
