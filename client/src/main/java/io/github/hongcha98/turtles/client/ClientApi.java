package io.github.hongcha98.turtles.client;

import io.github.hongcha98.turtles.client.config.TurtlesConfig;

public interface ClientApi extends LifeCycle {

    boolean createTopic(String topic, int queueNumber);

    boolean deleteTopic(String topic);

    TurtlesConfig getTurtlesConfig();

}

