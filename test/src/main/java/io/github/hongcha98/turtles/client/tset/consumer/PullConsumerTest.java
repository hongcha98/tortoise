package io.github.hongcha98.turtles.client.tset.consumer;

import io.github.hongcha98.turtles.client.config.TurtlesConfig;
import io.github.hongcha98.turtles.client.consumer.Consumer;
import io.github.hongcha98.turtles.client.consumer.PullDefaultConsumer;
import io.github.hongcha98.turtles.client.tset.dto.User;

public class PullConsumerTest {
    public static void main(String[] args) {
        TurtlesConfig turtlesConfig = new TurtlesConfig();
        turtlesConfig.setGroup("consumer-1");
        Consumer consumer = new PullDefaultConsumer(turtlesConfig);
        consumer.subscription("test-topic", message -> {
            User user = consumer.getProtocol().decode(message.getBody(), User.class);
            System.out.println(user);
            return true;
        });
        consumer.start();
    }
}
