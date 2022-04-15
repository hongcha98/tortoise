package io.github.hongcha98.tortoise.broker.task;

import io.github.hongcha98.tortoise.broker.TortoiseBroker;
import io.github.hongcha98.tortoise.broker.offset.OffsetManage;
import io.github.hongcha98.tortoise.broker.topic.TopicManage;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MessageDeleteTask extends AbstractTask {
    public MessageDeleteTask(TortoiseBroker tortoiseBroker) {
        super(tortoiseBroker);
    }

    @Override
    public void run() {
        try {
            TopicManage topicManage = getBroker().getTopicManage();
            OffsetManage offsetManage = getBroker().getOffsetManage();
            topicManage.getAllTopic().values().parallelStream().forEach(topic -> {
                Map<Integer, Integer> minOffsetMap = offsetManage.getMinOffset(topic.getName());
                topic.getQueueFiles().parallelStream().forEach(queueFile -> {
                    int queueId = queueFile.getId();
                    Integer minOffset = minOffsetMap.get(queueId);
                    if (minOffset != null) {
                        int remove = queueFile.removeTimeBefore(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(getBroker().getTortoiseConfig().getMessageSave()), minOffset);
                        if (remove != 0) {
                            offsetManage.offsetForward(topic.getName(), queueId, remove);
                        }
                    }
                });
            });
        } catch (Exception e) {
            LOG.error("message delete task");
        }
    }
}
