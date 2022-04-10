package io.github.hongcha98.turtles.broker.task;

import io.github.hongcha98.turtles.broker.TurtlesBroker;
import io.github.hongcha98.turtles.broker.topic.TopicManage;

public class TopicBrushTask extends AbstractTask {
    public TopicBrushTask(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
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
