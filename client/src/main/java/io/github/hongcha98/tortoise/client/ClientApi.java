package io.github.hongcha98.tortoise.client;

import io.github.hongcha98.remote.protocol.Protocol;
import io.github.hongcha98.tortoise.client.config.TortoiseConfig;

public interface ClientApi extends LifeCycle {

    boolean createTopic(String topic, int queueNumber);

    boolean deleteTopic(String topic);

    Protocol getProtocol();

    TortoiseConfig getTortoiseConfig();

}

