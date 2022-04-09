package io.github.hongcha98.turtles.client.tset.producer;

import io.github.hongcha98.turtles.client.config.TurtlesConfig;
import io.github.hongcha98.turtles.client.producer.DefaultProducer;
import io.github.hongcha98.turtles.client.producer.Producer;

import java.util.Scanner;

public class ProducerTest {
    public static void main(String[] args) {
        TurtlesConfig turtlesConfig = new TurtlesConfig();
        turtlesConfig.setGroupName("producer-test");
        Producer producer = new DefaultProducer(turtlesConfig);
        producer.start();
        producer.createTopic("test-topic", 8);
        Scanner scanner = new Scanner(System.in);
        while (true) {
            try {
                producer.send("test-topic", scanner.nextLine());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
