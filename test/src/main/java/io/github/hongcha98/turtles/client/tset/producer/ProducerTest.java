package io.github.hongcha98.turtles.client.tset.producer;

import io.github.hongcha98.turtles.client.config.TurtlesConfig;
import io.github.hongcha98.turtles.client.producer.DefaultProducer;
import io.github.hongcha98.turtles.client.producer.Producer;

import java.util.Scanner;

public class ProducerTest {
    public static void main(String[] args) {
        String topicName = "test-topic";
        TurtlesConfig turtlesConfig = new TurtlesConfig();
        turtlesConfig.setGroupName("producer-test");
        Producer producer = new DefaultProducer(turtlesConfig);
        producer.start();
        Scanner scanner = new Scanner(System.in);
        for (int i = 0; i < 1000000; i++) {
            producer.send(topicName, "hello world" + i);
        }
//        while (true) {
//            try {
//                producer.send("test-topic", scanner.nextLine());
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
    }
}
