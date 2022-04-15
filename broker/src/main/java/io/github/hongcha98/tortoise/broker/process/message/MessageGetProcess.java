package io.github.hongcha98.tortoise.broker.process.message;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.github.hongcha98.tortoise.broker.TortoiseBroker;
import io.github.hongcha98.tortoise.broker.context.ChannelContext;
import io.github.hongcha98.tortoise.broker.offset.OffsetManage;
import io.github.hongcha98.tortoise.broker.process.AbstractProcess;
import io.github.hongcha98.tortoise.broker.topic.QueueFile;
import io.github.hongcha98.tortoise.broker.topic.Topic;
import io.github.hongcha98.tortoise.broker.topic.TopicManage;
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
            Map<Integer, List<MessageEntry>> queueIdMessageMap = messageGetResponse.getQueueIdMessageMap();
            queueIds.parallelStream().forEach(queueId -> {
                QueueFile queueFile = tpc.getQueueFile(queueId);
                // current offset
                Integer offset = offsetManage.getOffset(topic, group, queueId);
                for (int i = 0; i < number; ) {
                    MessageEntry msg = queueFile.getMessage(offset, i == 0);
                    if (msg != null) {
                        offset = msg.getNextOffset();
                        // 首条数据如果超出消费次数,就跳过
                        if (i == 0) {
                            if (msg.getConsumptionTimes() > getBroker().getTortoiseConfig().getConsumerLimit()) {
                                offsetManage.casCommitOffset(topic, group, queueId, msg.getOffset(), offset);
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
