package io.github.hongcha98.turtles.client.consumer;

import io.github.hongcha98.turtles.client.ClientApi;

import java.util.Map;
import java.util.Set;

//TODO 暂时没想到好的实现方式
public interface Consumer extends ClientApi {

    void subscription(String topic, MessageListener messageListener);

    void subscription(Set<String> topics, MessageListener messageListener);

    Map<String, MessageListener> getMessageListenerMap();
}
