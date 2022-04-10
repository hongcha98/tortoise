package io.github.hongcha98.turtles.broker.process.message;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.github.hongcha98.turtles.broker.TurtlesBroker;
import io.github.hongcha98.turtles.broker.process.AbstractProcess;
import io.github.hongcha98.turtles.broker.topic.Topic;
import io.github.hongcha98.turtles.common.dto.message.MessageAddRequest;
import io.github.hongcha98.turtles.common.error.TopicException;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageAddProcess extends AbstractProcess {
    Map<String, AtomicInteger> topicPolling = new ConcurrentHashMap<>();


    public MessageAddProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        MessageAddRequest messageAddRequest = ProtocolUtils.decode(message, MessageAddRequest.class);
        Topic topic;
        try {
            topic = getBroker().getTopicManage().getTopic(messageAddRequest.getTopic());
        } catch (TopicException e) {
            responseException(channelHandlerContext, message, e);
            return;
        }
        io.github.hongcha98.turtles.common.dto.message.Message add = new io.github.hongcha98.turtles.common.dto.message.Message();
        add.setId(UUID.randomUUID().toString());
        add.setHeader(messageAddRequest.getHeader());
        add.setBody(messageAddRequest.getBody());
        ArrayList<Integer> queueIds = new ArrayList<>(topic.getQueuesId());
        AtomicInteger positionAtomic = topicPolling.computeIfAbsent(topic.getName(), t -> new AtomicInteger(0));
        int position = positionAtomic.getAndIncrement();
        if (position == Integer.MAX_VALUE) {
            positionAtomic.set(0);
            position = 0;
        }
        Integer queueId = queueIds.get(position % queueIds.size());
        int offset = topic.addMessage(queueId, add);
        LOG.info("message topic : {} , id :{} , queueId :{} add success , offset : {}", topic.getName(), add.getId(), queueId, offset);
        response(channelHandlerContext, message, add.getId());
    }
}
