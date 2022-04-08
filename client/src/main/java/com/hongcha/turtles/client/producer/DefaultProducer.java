package com.hongcha.turtles.client.producer;

import com.hongcha.remote.protocol.Protocol;
import com.hongcha.turtles.client.AbstractClientApi;
import com.hongcha.turtles.client.config.TurtlesConfig;
import com.hongcha.turtles.common.dto.message.MessageAddReq;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;


public class DefaultProducer extends AbstractClientApi implements Producer {
    public DefaultProducer(TurtlesConfig turtlesConfig) {
        super(turtlesConfig);
    }

    public DefaultProducer(TurtlesConfig turtlesConfig, Protocol protocol) {
        super(turtlesConfig, protocol);
    }

    @Override
    public String send(String topic, Map<String, String> header, Object msg) {
        MessageAddReq req = new MessageAddReq();
        req.setTopicName(topic);
        req.setHeader(header);
        req.setBody(getProtocol().encode(msg));
        return getCore().send(req);
    }

}
