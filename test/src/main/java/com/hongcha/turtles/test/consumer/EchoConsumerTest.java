package com.hongcha.turtles.test.consumer;

import com.hongcha.turtles.client.config.TurtlesConfig;
import com.hongcha.turtles.client.consumer.Consumer;
import com.hongcha.turtles.client.consumer.DefaultConsumer;

public class EchoConsumerTest {
    public static void main(String[] args) {
        TurtlesConfig turtlesConfig = new TurtlesConfig();
        turtlesConfig.setGroupName("consumer-2");
        Consumer consumer = new DefaultConsumer(turtlesConfig);
        consumer.subscription("test-topic", msg -> {
            System.out.println("msg = " + msg);
        });
        consumer.start();
    }
}
