package com.hongcha.turtles.client.consumer;

import com.hongcha.turtles.client.ClientApi;

import java.util.Map;
import java.util.Set;

public interface Consumer extends ClientApi {

    void subscription(String topic, MessageListener messageListener);

    void subscription(Set<String> topics, MessageListener messageListener);

    Map<String, MessageListener> getMessageListenerMap();
}
