package io.github.hongcha98.tortoise.broker.topic;

import io.github.hongcha98.tortoise.broker.config.TortoiseConfig;
import io.github.hongcha98.tortoise.common.dto.message.Message;
import org.junit.Before;
import org.junit.Test;

public class TopicTest {
    Topic topic;

    @Before
    public void init() {
        TortoiseConfig tortoiseConfig = new TortoiseConfig();
        topic = new Topic(tortoiseConfig.getStoragePath(), "test-topic", 8, tortoiseConfig.getCoding());
        topic.start();
    }

    @Test
    public void addMessage() {
        Message message = new Message();
        for (int i = 0; i < 100; i++) {
            System.out.println(topic.addMessage(message));
        }
    }


}
