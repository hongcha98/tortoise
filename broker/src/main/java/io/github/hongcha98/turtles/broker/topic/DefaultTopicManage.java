package io.github.hongcha98.turtles.broker.topic;

import io.github.hongcha98.turtles.broker.config.TurtlesConfig;
import io.github.hongcha98.turtles.broker.error.TurtlesException;
import io.github.hongcha98.turtles.broker.error.TopicNotExistsException;
import io.github.hongcha98.turtles.broker.utils.FileUtils;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultTopicManage implements TopicManage {
    private final Map<String, Topic> topicMap = new ConcurrentHashMap<>();

    private final TurtlesConfig turtlesConfig;


    public DefaultTopicManage(TurtlesConfig turtlesConfig) {
        this.turtlesConfig = turtlesConfig;
    }

    @Override
    public void start() {
        String storagePath = turtlesConfig.getStoragePath();
        File file = new File(storagePath);
        if (!file.exists()) {
            file.mkdir();
        }
        String[] list = file.list();
        for (String name : list) {
            File topicFile = new File(storagePath, name);
            if (topicFile.exists() && topicFile.isDirectory()) {
                int queueNumber = topicFile.list().length;
                if (queueNumber == 0) {
                    queueNumber = turtlesConfig.getQueueNumber();
                }
                addTopic(name, queueNumber);
            }
        }
    }


    @Override
    public boolean exists(String topicName) {
        return topicMap.containsKey(topicName);
    }

    @Override
    public Topic getTopic(String topicName) {
        Topic topic = topicMap.get(topicName);
        if (topic == null) {
            throw new TopicNotExistsException(topicName);
        }
        return topic;
    }

    @Override
    public void addTopic(String topicName, int queueNumber) {
        if (topicMap.containsKey(topicName)) {
            throw new TurtlesException("topic : " + topicName + " already exists");
        }
        Topic topic = new Topic(turtlesConfig.getStoragePath(), topicName, queueNumber, turtlesConfig.getCoding());
        topic.start();
        topicMap.put(topicName, topic);
    }

    @Override
    public void deleteTopic(String topicName) {
        Topic topic = topicMap.remove(topicName);
        if (topic != null) {
            topic.close();
            File file = new File(turtlesConfig.getStoragePath(), topicName);
            FileUtils.deleteDirectory(file);
        }
    }

    @Override
    public Map<String, Topic> getAllTopic() {
        return Collections.unmodifiableMap(topicMap);
    }

    public void close() {
        for (String name : topicMap.keySet()) {
            Topic topic = topicMap.remove(name);
            topic.close();
        }
    }


}
