package com.hongcha.turtles.broker.utils;

import java.io.File;

public class FileUtils {

    public static final void deleteDirectory(String path) {
        deleteDirectory(new File(path));
    }

    public static final void deleteDirectory(File file) {
        if (file.exists() && file.isDirectory()) {
            for (File f : file.listFiles()) {
                if (f.isDirectory()) {
                    deleteDirectory(f);
                } else {
                    f.delete();
                }
            }
            file.delete();
        }
    }

}
