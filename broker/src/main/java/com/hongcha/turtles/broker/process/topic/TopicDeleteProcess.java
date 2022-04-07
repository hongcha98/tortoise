package com.hongcha.turtles.broker.process.topic;

import com.hongcha.turtles.broker.TurtlesBroker;
import com.hongcha.turtles.broker.process.AbstractProcess;
import com.hongcha.turtles.broker.topic.TopicManage;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

public class TopicDeleteProcess extends AbstractProcess {
    public TopicDeleteProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        String topicName = ProtocolUtils.decode(message, String.class);
        TopicManage topicManage = getBroker().getTopicManage();
        topicManage.deleteTopic(topicName);
        response(channelHandlerContext, message, true);
    }

}
