package com.hongcha.hcmq.client.core;

import com.hongcha.hcmq.clent.producer.DefaultProducer;
import com.hongcha.hcmq.clent.producer.Producer;
import com.hongcha.hcmq.client.core.constant.ConstantTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ProducerTest {
    Producer producer;

    @Before
    public void init() {
        ConstantTest.HCMQ_CONFIG.setGroupName(ConstantTest.PRODUCER_GROUP_NAME);
        producer = new DefaultProducer(ConstantTest.HCMQ_CONFIG);
        producer.start();
    }

    @Test
    public void createTopic() {
        Assert.assertTrue(producer.createTopic(ConstantTest.TOPIC_NAME, 8));
    }

    @Test
    public void deleteTopic() {
        Assert.assertTrue(producer.deleteTopic(ConstantTest.TOPIC_NAME));
    }

    @Test
    public void send() {
        for (int i = 0; i < 10; i++) {
            Assert.assertNotNull(producer.send(ConstantTest.TOPIC_NAME, "hello world"));
        }

    }
}
