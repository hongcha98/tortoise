package io.github.hongcha98.turtles.broker.topic.queue;

import io.github.hongcha98.turtles.broker.constant.ConstantTest;
import io.github.hongcha98.turtles.common.dto.message.Message;
import io.github.hongcha98.turtles.common.dto.message.MessageInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class QueueFileTest {
    QueueFile queueFile;


    @Before
    public void init() {
        queueFile = new QueueFile(new File(ConstantTest.QUEUE_FILE_NAME), 0, new DefaultCoding());
    }

    @After
    public void close() {
        queueFile.close();
    }

    @Test
    public void addMessage() {
        for (int i = 1; i <= 10; i++) {
            Message message = new Message();
            message.setId(UUID.randomUUID().toString());
            message.setBody(("message index : " + i).getBytes(StandardCharsets.UTF_8));
            System.out.println(i + " message offset :" + queueFile.addMessage(message));
        }
    }

    @Test
    public void getMessage() {
        int offset = 4;
        for (int i = 0; i < 100; i++) {
            MessageInfo message = queueFile.getMessage(offset);
            System.out.println(message);
            offset = message.getNextOffset();
        }
    }
}
