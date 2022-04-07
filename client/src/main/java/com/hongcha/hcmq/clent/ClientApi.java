package com.hongcha.hcmq.clent;

import com.hongcha.hcmq.clent.config.HcmqConfig;

public interface ClientApi extends LifeCycle {

    boolean createTopic(String topic, int queueNumber);

    boolean deleteTopic(String topic);

    HcmqConfig getHcmqConfig();

}

