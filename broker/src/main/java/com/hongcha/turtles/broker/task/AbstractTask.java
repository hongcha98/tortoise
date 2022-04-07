package com.hongcha.turtles.broker.task;

import com.hongcha.turtles.broker.TurtlesBroker;

public abstract class AbstractTask implements Runnable {
    private final TurtlesBroker turtlesBroker;

    protected AbstractTask(TurtlesBroker turtlesBroker) {
        this.turtlesBroker = turtlesBroker;
    }

    @Override
    public void run() {
        do {
            doRun();
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
