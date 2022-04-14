package io.github.hongcha98.tortoise.broker.topic;

import io.github.hongcha98.tortoise.broker.config.TortoiseConfig;
import io.github.hongcha98.tortoise.common.dto.message.MessageEntry;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class TopicTest {
    Topic topic;

    @Before
    public void init() {
        TortoiseConfig tortoiseConfig = new TortoiseConfig();
        topic = new Topic(tortoiseConfig.getStoragePath(), "test-topic", 8);
        topic.start();
    }

    @Test
    public void addMessage() {
        MessageEntry messageEntry = new MessageEntry();
        for (int i = 0; i < 100; i++) {
            messageEntry.setBody(("hello world" + i).getBytes(StandardCharsets.UTF_8));
            int offset = topic.addMessage(1, messageEntry);
            MessageEntry message = topic.getMessage(1, offset);
            System.out.println(messageEntry);
            System.out.println(message);
        }
    }

    @Test
    public void removeTimeBefore() {
        System.out.println("topic.removeTimeBefore(1, System.currentTimeMillis()) = " + topic.removeTimeBefore(1, System.currentTimeMillis(), 1000));

    }

    @Test
    public void getMessage() {
        int offset = 4;
        MessageEntry messageEntry;
        while ((messageEntry = topic.getMessage(1, offset)) != null) {
            offset = messageEntry.getNextOffset();
            System.out.println(messageEntry);
        }
    }

}
