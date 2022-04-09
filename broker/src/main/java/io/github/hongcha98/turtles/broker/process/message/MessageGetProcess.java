package io.github.hongcha98.turtles.broker.process.message;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.github.hongcha98.turtles.broker.TurtlesBroker;
import io.github.hongcha98.turtles.broker.context.ChannelContext;
import io.github.hongcha98.turtles.broker.offset.OffsetManage;
import io.github.hongcha98.turtles.broker.process.AbstractProcess;
import io.github.hongcha98.turtles.broker.topic.Topic;
import io.github.hongcha98.turtles.common.dto.message.MessageGetReq;
import io.github.hongcha98.turtles.common.dto.message.MessageGetResp;
import io.github.hongcha98.turtles.common.dto.message.MessageInfo;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;
import java.util.Set;

public class MessageGetProcess extends AbstractProcess {
    public MessageGetProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        MessageGetReq messageGetReq = ProtocolUtils.decode(message, MessageGetReq.class);
        MessageGetResp messageGetResp = new MessageGetResp();
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel());
        String groupName = channelContext.getGroupName();
        String topicName = messageGetReq.getTopicName();
        Topic topic = getBroker().getTopicManage().getTopic(topicName);
        OffsetManage offsetManage = getBroker().getOffsetManage();
        Set<Integer> queueIds = getBroker().getSessionManage().getAllocate(topicName, groupName, channelHandlerContext.channel());
        Map<Integer, Integer> queueIdOffsetMap = offsetManage.getOffset(topicName, groupName);
        Map<Integer, MessageInfo> queueIdMessageMap = messageGetResp.getQueueIdMessageMap();
        for (Integer queueId : queueIds) {
            MessageInfo messageInfo = topic.getMessage(queueId, queueIdOffsetMap.get(queueId));
            if (messageInfo != null) {
                queueIdMessageMap.put(queueId, messageInfo);
            }
        }
        response(channelHandlerContext, message, messageGetResp);
    }
}
