package io.github.hongcha98.turtles.broker.process.topic;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.github.hongcha98.turtles.broker.TurtlesBroker;
import io.github.hongcha98.turtles.broker.process.AbstractProcess;
import io.netty.channel.ChannelHandlerContext;

public class TopicDeleteProcess extends AbstractProcess {
    public TopicDeleteProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        String topic = ProtocolUtils.decode(message, String.class);
        getBroker().getTopicManage().deleteTopic(topic);
        getBroker().getOffsetManage().deleteTopicOffset(topic);
        response(channelHandlerContext, message, true);
    }

}
