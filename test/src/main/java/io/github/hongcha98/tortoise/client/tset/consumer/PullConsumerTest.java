package io.github.hongcha98.tortoise.client.tset.consumer;

import io.github.hongcha98.tortoise.client.config.TortoiseConfig;
import io.github.hongcha98.tortoise.client.consumer.Consumer;
import io.github.hongcha98.tortoise.client.consumer.PullDefaultConsumer;
import io.github.hongcha98.tortoise.client.tset.dto.User;

public class PullConsumerTest {
    public static void main(String[] args) {
        TortoiseConfig tortoiseConfig = new TortoiseConfig();
        tortoiseConfig.setGroup("consumer-1");
        Consumer consumer = new PullDefaultConsumer(tortoiseConfig);
        consumer.subscription("test-topic", message -> {
            User user = consumer.getProtocol().decode(message.getBody(), User.class);
            System.out.println(user);
            return true;
        });
        consumer.start();
    }
}
