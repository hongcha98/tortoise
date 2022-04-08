package com.hongcha.turtles.client.producer;

import com.hongcha.turtles.client.ClientApi;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public interface Producer extends ClientApi {
    /**
     * 返回消息id
     *
     * @param topic
     * @param msg
     * @return
     */
    default String send(String topic, Object msg) {
        return send(topic, Collections.emptyMap(), msg);
    }

    /**
     * 返回消息id
     *
     * @param topic
     * @param msg
     * @return
     */
    String send(String topic, Map<String, String> header, Object msg);

    /**
     * 异步发送
     */
    default CompletableFuture<String> sendAsync(String topic, Object msg) {
        return sendAsync(topic, Collections.emptyMap(), msg);
    }

    /**
     * 异步发送
     */
    default CompletableFuture<String> sendAsync(String topic, Map<String, String> header, Object msg) {
        return CompletableFuture.supplyAsync(() -> send(topic, header, msg));
    }
}
