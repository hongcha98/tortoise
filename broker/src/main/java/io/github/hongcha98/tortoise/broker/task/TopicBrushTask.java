package io.github.hongcha98.tortoise.broker.task;

import io.github.hongcha98.tortoise.broker.TortoiseBroker;
import io.github.hongcha98.tortoise.broker.topic.TopicManage;

public class TopicBrushTask extends AbstractTask {
    public TopicBrushTask(TortoiseBroker tortoiseBroker) {
        super(tortoiseBroker);
    }

    @Override
    public void run() {
        try {
            TopicManage topicManage = getBroker().getTopicManage();
            topicManage.getAllTopic().forEach((name, topic) -> {
                topic.brush();
            });
        } catch (Exception e) {
            LOG.error("topic brush task error", e);
        }

    }
}
