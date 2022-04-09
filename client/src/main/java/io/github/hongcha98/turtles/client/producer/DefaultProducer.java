package io.github.hongcha98.turtles.client.producer;

import io.github.hongcha98.remote.protocol.Protocol;
import io.github.hongcha98.turtles.client.AbstractClientApi;
import io.github.hongcha98.turtles.client.config.TurtlesConfig;
import io.github.hongcha98.turtles.common.dto.message.MessageAddRequest;

import java.util.Map;
import java.util.concurrent.CompletableFuture;


public class DefaultProducer extends AbstractClientApi implements Producer {
    public DefaultProducer(TurtlesConfig turtlesConfig) {
        super(turtlesConfig);
    }

    public DefaultProducer(TurtlesConfig turtlesConfig, Protocol protocol) {
        super(turtlesConfig, protocol);
    }

    @Override
    public String send(String topic, Map<String, String> header, Object msg) {
        MessageAddRequest req = new MessageAddRequest();
        req.setTopicName(topic);
        req.setHeader(header);
        req.setBody(getProtocol().encode(msg));
        return getCore().send(req);
    }

    @Override
    public CompletableFuture<String> asyncSend(String topic, Map<String, String> header, Object msg) {
        return asyncSend(() -> send(topic, header, msg));
    }

}
