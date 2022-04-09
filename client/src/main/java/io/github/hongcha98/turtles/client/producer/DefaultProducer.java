package io.github.hongcha98.turtles.client.producer;

import io.github.hongcha98.remote.protocol.Protocol;
import io.github.hongcha98.turtles.client.AbstractClientApi;
import io.github.hongcha98.turtles.client.config.TurtlesConfig;
import io.github.hongcha98.turtles.common.dto.message.MessageAddReq;

import java.util.Map;


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
