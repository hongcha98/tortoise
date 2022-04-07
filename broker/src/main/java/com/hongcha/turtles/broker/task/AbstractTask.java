package com.hongcha.turtles.broker.task;

import com.hongcha.turtles.broker.TurtlesBroker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTask implements Runnable {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final TurtlesBroker turtlesBroker;

    protected AbstractTask(TurtlesBroker turtlesBroker) {
        this.turtlesBroker = turtlesBroker;
    }

    @Override
    public void run() {
        do {
            try {
                doRun();
            } catch (Exception e) {
                log.error("task run error", e);
            }

        } while (runForever());
    }

    protected abstract void doRun();

    protected boolean runForever() {
        return true;
    }

    public TurtlesBroker getBroker() {
        return turtlesBroker;
    }
}
