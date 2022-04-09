package io.github.hongcha98.turtles.broker;

import io.github.hongcha98.turtles.broker.config.TurtlesConfig;

public class TurtlesMain {
    public static void main(String[] args) {
        TurtlesConfig turtlesConfig = parse(args);
        TurtlesBroker turtlesBroker = new TurtlesBroker(turtlesConfig);
        turtlesBroker.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> turtlesBroker.close()));
    }

    private static TurtlesConfig parse(String[] args) {
        return new TurtlesConfig();
    }
}
