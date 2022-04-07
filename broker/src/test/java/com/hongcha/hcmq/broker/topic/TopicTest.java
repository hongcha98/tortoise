package com.hongcha.hcmq.broker.topic;

import com.hongcha.hcmq.broker.constant.ConstantTest;
import com.hongcha.hcmq.broker.queue.DefaultCoding;
import com.hongcha.hcmq.common.dto.message.Message;
import com.hongcha.hcmq.common.dto.message.MessageInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class TopicTest {
    Topic topic;


    @Before
    public void init() {
        topic = new Topic(ConstantTest.PATH, ConstantTest.TOPIC_NAME, 4, new DefaultCoding());
        topic.start();
    }

    @After
    public void close() {
        topic.close();
    }


    @Test
    public void addMessage() {
        int id = 0;
        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setId(UUID.randomUUID().toString());
            message.setBody(("hello world" + 2).getBytes(StandardCharsets.UTF_8));
            int offset = topic.addMessage(id, message);
            MessageInfo messageInfo = topic.getMessage(id, offset);
            Assert.assertEquals(message, messageInfo.getMessage());
        }
    }

}
