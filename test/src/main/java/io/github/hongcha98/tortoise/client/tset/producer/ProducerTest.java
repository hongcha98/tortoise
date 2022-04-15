package io.github.hongcha98.tortoise.client.tset.producer;

import io.github.hongcha98.tortoise.client.config.TortoiseConfig;
import io.github.hongcha98.tortoise.client.producer.DefaultProducer;
import io.github.hongcha98.tortoise.client.producer.Producer;
import io.github.hongcha98.tortoise.client.tset.dto.User;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ProducerTest {
    private static final String TOPIC = "test-topic";

    public static void main(String[] args) throws Exception {
        TortoiseConfig tortoiseConfig = new TortoiseConfig();
        tortoiseConfig.setGroup("producer-test");
        Producer producer = new DefaultProducer(tortoiseConfig);
        producer.start();
//        producer.createTopic("test-topic", 8);
        int frequency = 20;
        int number = 100000;
        // 预热线程池
        asyncSend(producer, number);
        List<Long> timeCostList = new LinkedList<>();
        for (int i = 0; i < frequency; i++) {
            timeCostList.add(asyncSend(producer, number));
        }
        Double avg = timeCostList.stream().collect(Collectors.averagingLong(Long::longValue));
        System.out.println("次数 : " + frequency + " , 数量 : " + number + ", 平均耗时 : " + avg);
        producer.close();
    }

    public static long asyncSend(Producer producer, int number) throws ExecutionException, InterruptedException {
        CompletableFuture<String>[] taskArray = new CompletableFuture[number];
        long start = System.currentTimeMillis();
        for (int i = 0; i < number; i++) {
            User user = new User();
            user.setName("hello world" + i);
            user.setAge(i);
            taskArray[i] = producer.asyncSend(TOPIC, new HashMap<>(), user);
        }
        CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(taskArray);
        voidCompletableFuture.get();
        return System.currentTimeMillis() - start;
    }
}
