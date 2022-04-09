package io.github.hongcha98.turtles.broker.task;

import io.github.hongcha98.turtles.broker.TurtlesBroker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTask implements Runnable {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final TurtlesBroker turtlesBroker;

    protected AbstractTask(TurtlesBroker turtlesBroker) {
        this.turtlesBroker = turtlesBroker;
    }

    public TurtlesBroker getBroker() {
        return turtlesBroker;
    }
}
