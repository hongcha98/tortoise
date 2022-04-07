package com.hongcha.turtles.broker.process.message;

import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
import com.hongcha.turtles.broker.TurtlesBroker;
import com.hongcha.turtles.broker.context.ChannelContext;
import com.hongcha.turtles.broker.offset.OffsetManage;
import com.hongcha.turtles.broker.process.AbstractProcess;
import com.hongcha.turtles.broker.topic.Topic;
import com.hongcha.turtles.common.dto.message.MessageGetReq;
import com.hongcha.turtles.common.dto.message.MessageGetResp;
import com.hongcha.turtles.common.dto.message.MessageInfo;
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
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel());
        Topic topic = getBroker().getTopicManage().getTopic(messageGetReq.getTopicName());
        OffsetManage offsetManage = getBroker().getOffsetManage();
        Set<Integer> queueIds = getBroker().getSessionManage().getAllocate(topic.getName(), channelContext.getGroupName(), channelHandlerContext.channel());
        Map<Integer, Integer> queueIdOffsetMap = offsetManage.getOffset(topic.getName(), channelContext.getGroupName());
        MessageGetResp messageGetResp = new MessageGetResp();
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
