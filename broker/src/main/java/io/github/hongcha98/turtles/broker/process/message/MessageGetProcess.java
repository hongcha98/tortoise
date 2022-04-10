package io.github.hongcha98.turtles.broker.process.message;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.github.hongcha98.turtles.broker.TurtlesBroker;
import io.github.hongcha98.turtles.broker.context.ChannelContext;
import io.github.hongcha98.turtles.broker.offset.OffsetManage;
import io.github.hongcha98.turtles.broker.process.AbstractProcess;
import io.github.hongcha98.turtles.broker.topic.Topic;
import io.github.hongcha98.turtles.broker.topic.TopicManage;
import io.github.hongcha98.turtles.common.dto.message.MessageGetRequest;
import io.github.hongcha98.turtles.common.dto.message.MessageGetResponse;
import io.github.hongcha98.turtles.common.dto.message.MessageInfo;
import io.netty.channel.ChannelHandlerContext;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MessageGetProcess extends AbstractProcess {
    public MessageGetProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        MessageGetRequest messageGetRequest = ProtocolUtils.decode(message, MessageGetRequest.class);
        String topic = messageGetRequest.getTopic();
        int number = messageGetRequest.getNumber();
        MessageGetResponse messageGetResponse = new MessageGetResponse();
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel());
        String group = channelContext.getGroup();
        TopicManage topicManage = getBroker().getTopicManage();
        if (group != null && topicManage.exists(topic)) {
            Topic tpc = getBroker().getTopicManage().getTopic(topic);
            OffsetManage offsetManage = getBroker().getOffsetManage();
            Set<Integer> queueIds = getBroker().getSessionManage().getAllocate(topic, group, channelHandlerContext.channel());
            Map<Integer, Integer> queueIdOffsetMap = offsetManage.getOffset(topic, group);
            Map<Integer, List<MessageInfo>> queueIdMessageMap = messageGetResponse.getQueueIdMessageMap();
            queueIds.parallelStream().forEach(queueId -> {
                // current offset
                Integer offset = queueIdOffsetMap.get(queueId);
                for (int i = 0; i < number; i++) {
                    MessageInfo messageInfo = tpc.getMessage(queueId, offset);
                    if (messageInfo != null) {
                        queueIdMessageMap.computeIfAbsent(queueId, q -> new LinkedList<>()).add(messageInfo);
                        offset = messageInfo.getNextOffset();
                    }
                }
            });
        }
        response(channelHandlerContext, message, messageGetResponse);
    }
}
