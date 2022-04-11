package io.github.hongcha98.tortoise.broker;

import io.github.hongcha98.tortoise.broker.config.TortoiseConfig;

public class TortoiseMain {
    public static void main(String[] args) {
        TortoiseConfig tortoiseConfig = parse(args);
        TortoiseBroker tortoiseBroker = new TortoiseBroker(tortoiseConfig);
        tortoiseBroker.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> tortoiseBroker.close()));
    }

    private static TortoiseConfig parse(String[] args) {
        return new TortoiseConfig();
    }
}
