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
            QueueFile queueFile = topic.getNextStoreQueueFile();
            int offset = queueFile.addMessage(messageEntry);
            MessageEntry message = queueFile.getMessage(offset);
            System.out.println(messageEntry);
            System.out.println(message);
        }
    }

    @Test
    public void removeTimeBefore() {

    }

    @Test
    public void getMessage() {
        int offset = 4;
        MessageEntry messageEntry;
        QueueFile queueFile = topic.getQueueFile(1);
        while ((messageEntry = queueFile.getMessage(offset)) != null) {
            offset = messageEntry.getNextOffset();
            System.out.println(messageEntry);
        }
    }

}
