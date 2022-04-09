package io.github.hongcha98.turtles.client;

import io.github.hongcha98.turtles.client.producer.DefaultProducer;
import io.github.hongcha98.turtles.client.producer.Producer;
import io.github.hongcha98.turtles.client.constant.ConstantTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ProducerTest {
    Producer producer;

    @Before
    public void init() {
        ConstantTest.TURTLES_CONFIG.setGroupName(ConstantTest.PRODUCER_GROUP_NAME);
        producer = new DefaultProducer(ConstantTest.TURTLES_CONFIG);
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
