package com.hongcha.hcmq.broker.process.message;

import com.hongcha.hcmq.broker.Broker;
import com.hongcha.hcmq.broker.process.AbstractProcess;
import com.hongcha.hcmq.broker.topic.Topic;
import com.hongcha.hcmq.common.dto.message.MessageGetReq;
import com.hongcha.hcmq.common.dto.message.MessageInfo;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

public class MessageGetProcess extends AbstractProcess {


    public MessageGetProcess(Broker broker) {
        super(broker);
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
