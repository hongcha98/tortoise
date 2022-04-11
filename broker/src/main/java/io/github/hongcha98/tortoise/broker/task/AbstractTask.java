package io.github.hongcha98.tortoise.broker.task;

import io.github.hongcha98.tortoise.broker.TortoiseBroker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTask implements Runnable {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final TortoiseBroker tortoiseBroker;

    protected AbstractTask(TortoiseBroker tortoiseBroker) {
        this.tortoiseBroker = tortoiseBroker;
    }

    public TortoiseBroker getBroker() {
        return tortoiseBroker;
    }
}
