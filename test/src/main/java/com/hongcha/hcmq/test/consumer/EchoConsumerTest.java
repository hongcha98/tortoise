package com.hongcha.hcmq.test.consumer;

import com.hongcha.hcmq.clent.config.HcmqConfig;
import com.hongcha.hcmq.clent.consumer.Consumer;
import com.hongcha.hcmq.clent.consumer.DefaultConsumer;

public class EchoConsumerTest {
    public static void main(String[] args) {
        HcmqConfig hcmqConfig = new HcmqConfig();
        hcmqConfig.setGroupName("consumer-2");
        Consumer consumer = new DefaultConsumer(hcmqConfig);
        consumer.subscription("test-topic", msg -> {
            System.out.println("msg = " + msg);
        });
        consumer.start();
    }
}
