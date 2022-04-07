package com.hongcha.turtles.client;

import com.hongcha.turtles.client.config.TurtlesConfig;

public interface ClientApi extends LifeCycle {

    boolean createTopic(String topic, int queueNumber);

    boolean deleteTopic(String topic);

    TurtlesConfig getTurtlesConfig();

}

