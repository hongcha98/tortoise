package io.github.hongcha98.turtles.broker.process.topic;

import io.github.hongcha98.turtles.broker.TurtlesBroker;
import io.github.hongcha98.turtles.broker.process.AbstractProcess;
import io.github.hongcha98.turtles.broker.topic.TopicManage;
import io.github.hongcha98.turtles.common.dto.topic.TopicCreateRequest;
import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

public class TopicCreateProcess extends AbstractProcess {
    public TopicCreateProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        TopicCreateRequest topicCreateRequest = ProtocolUtils.decode(message, TopicCreateRequest.class);
        String topicName = topicCreateRequest.getTopicName();
        int queueNumber = topicCreateRequest.getQueueNumber();
        TopicManage topicManage = getBroker().getTopicManage();
        if (topicManage.exists(topicName)) {
            responseException(channelHandlerContext, message, new IllegalStateException("topic " + topicName + " exists"));
        } else {
            boolean flag = false;
            try {
                topicManage.addTopic(topicName, queueNumber);
                flag = true;
            } catch (Exception e) {
                log.error("topic create error", e);
            }
            response(channelHandlerContext, message, flag);
        }
    }

}
