package com.hongcha.turtles.client;

import com.hongcha.turtles.client.config.TurtlesConfig;
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
