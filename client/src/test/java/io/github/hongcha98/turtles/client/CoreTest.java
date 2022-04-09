package io.github.hongcha98.turtles.client;

import io.github.hongcha98.turtles.client.config.TurtlesConfig;
import org.junit.Before;
import org.junit.Test;

public class CoreTest {
    Core core;

    TurtlesConfig turtlesConfig;

    @Before
    public void init() {
        turtlesConfig = new TurtlesConfig();
        core = new DefaultCore(turtlesConfig);
    }

    @Test
    public void login() {

    }


}
