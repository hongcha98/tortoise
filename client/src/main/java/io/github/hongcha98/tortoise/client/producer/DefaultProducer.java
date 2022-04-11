package io.github.hongcha98.tortoise.client.producer;

import io.github.hongcha98.remote.protocol.Protocol;
import io.github.hongcha98.tortoise.client.AbstractClientApi;
import io.github.hongcha98.tortoise.client.config.TortoiseConfig;
import io.github.hongcha98.tortoise.common.dto.message.request.MessageAddRequest;

import java.util.Map;
import java.util.concurrent.CompletableFuture;


public class DefaultProducer extends AbstractClientApi implements Producer {
    public DefaultProducer(TortoiseConfig tortoiseConfig) {
        super(tortoiseConfig);
    }

    public DefaultProducer(TortoiseConfig tortoiseConfig, Protocol protocol) {
        super(tortoiseConfig, protocol);
    }

    @Override
    public String send(String topic, Map<String, String> header, Object msg) {
        MessageAddRequest req = new MessageAddRequest();
        req.setTopic(topic);
        req.setHeader(header);
        req.setBody(getProtocol().encode(msg));
        return getCore().send(req);
    }

    @Override
    public CompletableFuture<String> asyncSend(String topic, Map<String, String> header, Object msg) {
        return asyncSend(() -> send(topic, header, msg));
    }

}
