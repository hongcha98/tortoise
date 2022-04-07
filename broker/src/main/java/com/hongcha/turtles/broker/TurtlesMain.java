package com.hongcha.turtles.broker;

import com.hongcha.turtles.broker.config.TurtlesConfig;

public class TurtlesMain {
    public static void main(String[] args) {
        TurtlesConfig turtlesConfig = parse(args);
        TurtlesBroker turtlesBroker = new TurtlesBroker(turtlesConfig);
        turtlesBroker.start();
    }

    private static TurtlesConfig parse(String[] args) {
        return new TurtlesConfig();
    }
}
