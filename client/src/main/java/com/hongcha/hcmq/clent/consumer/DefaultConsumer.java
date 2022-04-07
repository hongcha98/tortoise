package com.hongcha.hcmq.clent.consumer;

import com.hongcha.hcmq.clent.AbstractClientApi;
import com.hongcha.hcmq.clent.config.HcmqConfig;
import com.hongcha.remote.protocol.Protocol;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultConsumer extends AbstractClientApi implements Consumer {

    private Map<String, MessageListener> messageListenerMap = new ConcurrentHashMap<>();

    public DefaultConsumer(HcmqConfig hcmqConfig) {
        super(hcmqConfig);

    }


    public DefaultConsumer(HcmqConfig hcmqConfig, Protocol protocol) {
        super(hcmqConfig, protocol);

    }

    public Map<String, MessageListener> getMessageListenerMap() {
        return Collections.unmodifiableMap(messageListenerMap);
    }

    @Override
    protected void doStart() throws Exception {

    }

    @Override
    protected void doClose() {

    }

    @Override
    public void subscription(String topic, MessageListener messageListener) {
        messageListenerMap.put(topic, messageListener);
    }

    @Override
    public void subscription(Set<String> topics, MessageListener messageListener) {
        for (String topic : topics) {
            subscription(topic, messageListener);
        }
    }
}
