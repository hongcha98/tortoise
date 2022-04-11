package io.github.hongcha98.tortoise.broker.topic;

import io.github.hongcha98.tortoise.broker.config.TortoiseConfig;
import io.github.hongcha98.tortoise.broker.utils.FileUtils;
import io.github.hongcha98.tortoise.common.error.TopicNotExistsException;
import io.github.hongcha98.tortoise.common.error.TortoiseException;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultTopicManage implements TopicManage {
    private final Map<String, Topic> topicMap = new ConcurrentHashMap<>();

    private final TortoiseConfig tortoiseConfig;


    public DefaultTopicManage(TortoiseConfig tortoiseConfig) {
        this.tortoiseConfig = tortoiseConfig;
    }

    @Override
    public void start() {
        String storagePath = tortoiseConfig.getStoragePath();
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
                    queueNumber = tortoiseConfig.getQueueNumber();
                }
                addTopic(name, queueNumber);
            }
        }
    }


    @Override
    public boolean exists(String topic) {
        return topicMap.containsKey(topic);
    }

    @Override
    public Topic getTopic(String topic) {
        Topic tpc = topicMap.get(topic);
        if (tpc == null) {
            throw new TopicNotExistsException(topic);
        }
        return tpc;
    }

    @Override
    public void addTopic(String topic, int queueNumber) {
        if (topicMap.containsKey(topic)) {
            throw new TortoiseException("topic : " + topic + " already exists");
        }
        Topic tpc = new Topic(tortoiseConfig.getStoragePath(), topic, queueNumber, tortoiseConfig.getCoding());
        tpc.start();
        topicMap.put(topic, tpc);
    }

    @Override
    public void deleteTopic(String topic) {
        Topic tpc = topicMap.remove(topic);
        if (topic != null) {
            tpc.close();
            File file = new File(tortoiseConfig.getStoragePath(), topic);
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
