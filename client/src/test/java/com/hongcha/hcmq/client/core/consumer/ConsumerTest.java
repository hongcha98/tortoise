package com.hongcha.hcmq.client.core.consumer;

import com.hongcha.hcmq.clent.consumer.Consumer;
import com.hongcha.hcmq.clent.consumer.DefaultConsumer;
import com.hongcha.hcmq.client.core.constant.ConstantTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ConsumerTest {
    Consumer consumer;

    @Before
    public void init() {
        ConstantTest.HCMQ_CONFIG.setGroupName(ConstantTest.CONSUMER_GROUP_NAME);
        consumer = new DefaultConsumer(ConstantTest.HCMQ_CONFIG);
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
