package com.hongcha.hcmq.test.producer;

import com.hongcha.hcmq.clent.config.HcmqConfig;
import com.hongcha.hcmq.clent.producer.DefaultProducer;
import com.hongcha.hcmq.clent.producer.Producer;

import java.util.Scanner;

public class ProducerTest {
    public static void main(String[] args) {
        HcmqConfig hcmqConfig = new HcmqConfig();
        hcmqConfig.setGroupName("producer");
        Producer producer = new DefaultProducer(hcmqConfig);
        producer.start();
        producer.createTopic("test-topic", 1);
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
