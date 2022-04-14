package io.github.hongcha98.tortoise.broker.task;

import io.github.hongcha98.tortoise.broker.TortoiseBroker;
import io.github.hongcha98.tortoise.broker.constant.Constant;
import io.github.hongcha98.tortoise.broker.offset.OffsetManage;
import io.github.hongcha98.tortoise.broker.topic.Topic;
import io.github.hongcha98.tortoise.broker.topic.TopicManage;
import io.github.hongcha98.tortoise.common.dto.message.MessageEntry;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 延时消息task
 */
public class DelayMessageTask extends AbstractTask {
    private static final Map<Integer, Long> DELAY_LEVEL_TIME_MAP = new HashMap<>();

    static {
        DELAY_LEVEL_TIME_MAP.put(1, TimeUnit.SECONDS.toMillis(1));
        DELAY_LEVEL_TIME_MAP.put(2, TimeUnit.SECONDS.toMillis(2));
        DELAY_LEVEL_TIME_MAP.put(3, TimeUnit.SECONDS.toMillis(3));
        DELAY_LEVEL_TIME_MAP.put(4, TimeUnit.SECONDS.toMillis(5));
        DELAY_LEVEL_TIME_MAP.put(5, TimeUnit.SECONDS.toMillis(10));
        DELAY_LEVEL_TIME_MAP.put(6, TimeUnit.SECONDS.toMillis(15));
        DELAY_LEVEL_TIME_MAP.put(7, TimeUnit.SECONDS.toMillis(30));
        DELAY_LEVEL_TIME_MAP.put(8, TimeUnit.MINUTES.toMillis(1));
        DELAY_LEVEL_TIME_MAP.put(9, TimeUnit.MINUTES.toMillis(2));
        DELAY_LEVEL_TIME_MAP.put(10, TimeUnit.MINUTES.toMillis(3));
        DELAY_LEVEL_TIME_MAP.put(11, TimeUnit.MINUTES.toMillis(5));
        DELAY_LEVEL_TIME_MAP.put(12, TimeUnit.MINUTES.toMillis(10));
        DELAY_LEVEL_TIME_MAP.put(13, TimeUnit.MINUTES.toMillis(15));
        DELAY_LEVEL_TIME_MAP.put(14, TimeUnit.MINUTES.toMillis(30));
        DELAY_LEVEL_TIME_MAP.put(15, TimeUnit.HOURS.toMillis(1));
        DELAY_LEVEL_TIME_MAP.put(16, TimeUnit.HOURS.toMillis(2));
    }

    public DelayMessageTask(TortoiseBroker tortoiseBroker) {
        super(tortoiseBroker);
    }

    @Override
    public void run() {
        try {
            TopicManage topicManage = getBroker().getTopicManage();
            OffsetManage offsetManage = getBroker().getOffsetManage();
            Topic tpc = topicManage.getTopic(Constant.DELAY_TOPIC);
            tpc.getQueuesId().parallelStream().forEach(queueId -> {
                int offset = offsetManage.getOffset(Constant.DELAY_TOPIC, Constant.DELAY_GROUP, queueId);
                MessageEntry messageEntry;
                while ((messageEntry = tpc.getMessage(queueId, offset)) != null) {
                    offset = messageEntry.getNextOffset();
                    Map<String, String> header = messageEntry.getHeader();
                    // 是否超时
                    if (System.currentTimeMillis() >= messageEntry.getCreateTime() + DELAY_LEVEL_TIME_MAP.get(queueId + 1)) {
                        String topic = header.remove(Constant.DELAY_HEADER_TOPIC);
                        Topic targetTopic = topicManage.getTopic(topic);
                        targetTopic.addMessage(messageEntry);
                        offsetManage.commitOffset(Constant.DELAY_TOPIC, Constant.DELAY_GROUP, queueId, offset);
                    } else {
                        break;
                    }
                }
            });
        } catch (Exception e) {
            LOG.error("delay message task error", e);
        }
    }
}
