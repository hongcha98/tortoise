package com.hongcha.turtles.client.consumer;

import com.hongcha.turtles.client.constant.ConstantTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ConsumerTest {
    Consumer consumer;

    @Before
    public void init() {
        ConstantTest.TURTLES_CONFIG.setGroupName(ConstantTest.CONSUMER_GROUP_NAME);
        consumer = new PullDefaultConsumer(ConstantTest.TURTLES_CONFIG);
    }

    @Test
    public void listener() throws InterruptedException, IOException {
        consumer.subscription(ConstantTest.TOPIC_NAME, (message -> {
            System.out.println("message = " + message);
        }));
        consumer.start();
        System.in.read();
    }
}
