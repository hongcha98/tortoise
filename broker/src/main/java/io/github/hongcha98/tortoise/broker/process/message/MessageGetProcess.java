package io.github.hongcha98.tortoise.broker.process.message;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.github.hongcha98.tortoise.broker.context.ChannelContext;
import io.github.hongcha98.tortoise.broker.offset.OffsetManage;
import io.github.hongcha98.tortoise.broker.process.AbstractProcess;
import io.github.hongcha98.tortoise.broker.topic.Topic;
import io.github.hongcha98.tortoise.broker.topic.TopicManage;
import io.github.hongcha98.tortoise.broker.TortoiseBroker;
import io.github.hongcha98.tortoise.common.dto.message.MessageEntry;
import io.github.hongcha98.tortoise.common.dto.message.request.MessageGetRequest;
import io.github.hongcha98.tortoise.common.dto.message.response.MessageGetResponse;
import io.netty.channel.ChannelHandlerContext;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MessageGetProcess extends AbstractProcess {
    public MessageGetProcess(TortoiseBroker tortoiseBroker) {
        super(tortoiseBroker);
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
            Map<Integer, List<MessageEntry>> queueIdMessageMap = messageGetResponse.getQueueIdMessageMap();
            queueIds.parallelStream().forEach(queueId -> {
                // current offset
                Integer offset = queueIdOffsetMap.get(queueId);
                for (int i = 0; i < number; ) {
                    MessageEntry msg = tpc.getMessage(queueId, offset, i == 0);
                    if (msg != null) {
                        offset = msg.getNextOffset();
                        // 首条数据如果超出消费次数,就跳过
                        if (i == 0) {
                            if (msg.getConsumptionTimes() > getBroker().getTortoiseConfig().getConsumerLimit()) {
                                offsetManage.commitOffset(topic, group, queueId, offset);
                                continue;
                            }
                        }
                        queueIdMessageMap.computeIfAbsent(queueId, q -> new LinkedList<>()).add(msg);
                        i++;
                    } else {
                        break;
                    }

                }
            });
        }
        response(channelHandlerContext, message, messageGetResponse);
    }
}
