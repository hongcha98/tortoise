package com.hongcha.turtles.broker.process.message;

import com.hongcha.turtles.broker.TurtlesBroker;
import com.hongcha.turtles.broker.error.TopicException;
import com.hongcha.turtles.broker.process.AbstractProcess;
import com.hongcha.turtles.broker.topic.Topic;
import com.hongcha.turtles.common.dto.message.MessageAddReq;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class MessageAddProcess extends AbstractProcess {
    public MessageAddProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        MessageAddReq messageAddReq = ProtocolUtils.decode(message, MessageAddReq.class);
        Topic topic;
        try {
            topic = getBroker().getTopicManage().getTopic(messageAddReq.getTopicName());
        } catch (TopicException e) {
            responseException(channelHandlerContext, message, e);
            return;
        }
        com.hongcha.turtles.common.dto.message.Message add = new com.hongcha.turtles.common.dto.message.Message();
        add.setId(UUID.randomUUID().toString());
        add.setHeader(messageAddReq.getHeader());
        add.setBody(messageAddReq.getBody());
        Set<Integer> queuesId = topic.getQueuesId();
        int size = queuesId.size();
        int randomIndex = new Random().nextInt(size);
        Integer id = new ArrayList<>(queuesId).get(randomIndex);
        int offset = topic.addMessage(id, add);
        log.info("message topic : {} , id :{} , queueId :{} add success , offset : {}", topic.getName(), add.getId(), id, offset);
        response(channelHandlerContext, message, true);
    }
}
