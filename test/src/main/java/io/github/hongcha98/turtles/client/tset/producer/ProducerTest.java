package io.github.hongcha98.turtles.client.tset.producer;

import io.github.hongcha98.turtles.client.config.TurtlesConfig;
import io.github.hongcha98.turtles.client.producer.DefaultProducer;
import io.github.hongcha98.turtles.client.producer.Producer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ProducerTest {
    private static final String TOPIC_NAME = "test-topic";

    public static void main(String[] args) throws Exception {
        TurtlesConfig turtlesConfig = new TurtlesConfig();
        turtlesConfig.setGroupName("producer-test");
        Producer producer = new DefaultProducer(turtlesConfig);
        producer.start();
//        producer.createTopic(TOPIC_NAME, 8);
        asyncSend(producer, 100000);
        producer.close();
    }

    public static void send(Producer producer, int number) {
        long l = System.currentTimeMillis();
        for (int i = 0; i < number; i++) {
            producer.send(TOPIC_NAME, "hello world" + i);
        }
        System.out.println("time cost :" + (System.currentTimeMillis() - l));
    }

    public static void asyncSend(Producer producer, int number) throws ExecutionException, InterruptedException {
        CompletableFuture<String>[] taskArray = new CompletableFuture[number];
        long l = System.currentTimeMillis();
        for (int i = 0; i < number; i++) {
            taskArray[i] = producer.asyncSend(TOPIC_NAME, "hello world" + i);
        }
        CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(taskArray);
        voidCompletableFuture.get();
        System.out.println("number : " + number + " time cost :" + (System.currentTimeMillis() - l));
    }
}
