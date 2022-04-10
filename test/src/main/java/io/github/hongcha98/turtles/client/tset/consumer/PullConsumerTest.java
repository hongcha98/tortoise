package io.github.hongcha98.turtles.client.tset.consumer;

import io.github.hongcha98.turtles.client.config.TurtlesConfig;
import io.github.hongcha98.turtles.client.consumer.Consumer;
import io.github.hongcha98.turtles.client.consumer.PullDefaultConsumer;

public class PullConsumerTest {
    public static void main(String[] args) {
        TurtlesConfig turtlesConfig = new TurtlesConfig();
        turtlesConfig.setGroup("consumer-1");
        Consumer consumer = new PullDefaultConsumer(turtlesConfig);
        consumer.subscription("test-topic", message -> {
            System.out.println("message = " + message);
            return true;
        });
        consumer.start();
    }
}
