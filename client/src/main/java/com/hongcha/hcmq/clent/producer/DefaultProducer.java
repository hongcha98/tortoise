package com.hongcha.hcmq.clent.producer;

import com.hongcha.hcmq.clent.AbstractClientApi;
import com.hongcha.hcmq.clent.config.HcmqConfig;
import com.hongcha.hcmq.common.dto.message.MessageAddReq;
import com.hongcha.remote.protocol.Protocol;

import java.util.Collections;
import java.util.Map;


public class DefaultProducer extends AbstractClientApi implements Producer {


    public DefaultProducer(HcmqConfig hcmqConfig) {
        super(hcmqConfig);
    }

    public DefaultProducer(HcmqConfig hcmqConfig, Protocol protocol) {
        super(hcmqConfig, protocol);
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
