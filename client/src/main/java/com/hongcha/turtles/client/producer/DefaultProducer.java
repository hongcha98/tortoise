package com.hongcha.turtles.client.producer;

import com.hongcha.turtles.client.AbstractClientApi;
import com.hongcha.turtles.client.config.TurtlesConfig;
import com.hongcha.turtles.common.dto.message.MessageAddReq;
import com.hongcha.remote.protocol.Protocol;

import java.util.Collections;
import java.util.Map;


public class DefaultProducer extends AbstractClientApi implements Producer {


    public DefaultProducer(TurtlesConfig turtlesConfig) {
        super(turtlesConfig);
    }

    public DefaultProducer(TurtlesConfig turtlesConfig, Protocol protocol) {
        super(turtlesConfig, protocol);
    }

    public String send(String topic, Object msg) {
        return send(topic, Collections.emptyMap(), msg);
    }

    @Override
    public String send(String topic, Map<String, String> header, Object msg) {
        MessageAddReq req = new MessageAddReq();
        req.setTopicName(topic);
        req.setHeader(header);
        req.setBody(getProtocol().encode(msg));
        return getCore().send(req);
    }


    @Override
    protected void doStart() throws Exception {

    }

    @Override
    protected void doClose() {

    }
}
