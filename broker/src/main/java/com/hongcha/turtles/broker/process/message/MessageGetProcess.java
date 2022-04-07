package com.hongcha.turtles.broker.process.message;

import com.hongcha.turtles.broker.TurtlesBroker;
import com.hongcha.turtles.broker.process.AbstractProcess;
import com.hongcha.turtles.broker.topic.Topic;
import com.hongcha.turtles.common.dto.message.MessageGetReq;
import com.hongcha.turtles.common.dto.message.MessageInfo;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

public class MessageGetProcess extends AbstractProcess {


    public MessageGetProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        MessageGetReq messageGetReq = ProtocolUtils.decode(message, MessageGetReq.class);
        Topic topic = getBroker().getTopicManage().getTopic(messageGetReq.getTopicName());
        MessageInfo messageInfo = topic.getMessage(messageGetReq.getId(), messageGetReq.getOffset());
        if (messageInfo == null) {
            messageInfo = new MessageInfo(null, -1, -1);
        }
        response(channelHandlerContext, message, messageInfo);
    }
}
