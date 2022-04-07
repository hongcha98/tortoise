package com.hongcha.turtles.test.producer;

import com.hongcha.turtles.client.config.TurtlesConfig;
import com.hongcha.turtles.client.producer.DefaultProducer;
import com.hongcha.turtles.client.producer.Producer;

import java.util.Scanner;

public class ProducerTest {
    public static void main(String[] args) {
        TurtlesConfig turtlesConfig = new TurtlesConfig();
        turtlesConfig.setGroupName("producer");
        Producer producer = new DefaultProducer(turtlesConfig);
        producer.start();
//        producer.createTopic("test-topic", 1);
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
