package com.hongcha.turtles.client.producer;

import com.hongcha.turtles.client.ClientApi;

import java.util.Map;

public interface Producer extends ClientApi {
    /**
     * 返回消息id
     *
     * @param topic
     * @param msg
     * @return
     */
    String send(String topic, Object msg);

    /**
     * 返回消息id
     *
     * @param topic
     * @param msg
     * @return
     */
    String send(String topic, Map<String, String> header, Object msg);

}
