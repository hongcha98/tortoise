package com.hongcha.turtles.test.producer;

import com.hongcha.turtles.client.config.TurtlesConfig;
import com.hongcha.turtles.client.producer.DefaultProducer;
import com.hongcha.turtles.client.producer.Producer;

import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

public class ProducerTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TurtlesConfig turtlesConfig = new TurtlesConfig();
        turtlesConfig.setGroupName("producer");
        Producer producer = new DefaultProducer(turtlesConfig);
        producer.start();
        long start = System.currentTimeMillis();
        ExecutorService executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(100);
        List<CompletableFuture<String>> taskAll = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            taskAll.add(producer.sendAsync("test-topic", "hello world" + finalI));
        }
        CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(taskAll.toArray(new CompletableFuture[taskAll.size()]));
        voidCompletableFuture.get();
        System.out.println("send ok : time cost :" + (System.currentTimeMillis() - start));
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
