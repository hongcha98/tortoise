package io.github.hongcha98.turtles.broker.topic;

import io.github.hongcha98.turtles.broker.config.TurtlesConfig;
import io.github.hongcha98.turtles.common.dto.message.Message;
import org.junit.Before;
import org.junit.Test;

public class TopicTest {
    Topic topic;

    @Before
    public void init() {
        TurtlesConfig turtlesConfig = new TurtlesConfig();
        topic = new Topic(turtlesConfig.getStoragePath(), "test-topic", 8, turtlesConfig.getCoding());
        topic.start();
    }

    @Test
    public void addMessage(){
        Message message = new Message();
        for (int i = 0; i < 100; i++) {
            System.out.println(topic.addMessage(0, message));
        }
    }


}
