package io.github.hongcha98.tortoise.broker.topic;

import io.github.hongcha98.tortoise.broker.LifeCycle;
import io.github.hongcha98.tortoise.broker.constant.Constant;
import io.github.hongcha98.tortoise.common.error.TortoiseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


public class Topic implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(Topic.class);
    /**
     * 存储位置
     */
    private final String path;
    /**
     * 主题名字
     */
    private final String name;
    /**
     * 轮询位置
     */
    private AtomicInteger polling = new AtomicInteger(0);
    /**
     * 队列列表
     */
    private Map<Integer/* id */, QueueFile> queueFileMap;
    /**
     * 队列数量
     */
    private int queueNumber;

    public Topic(String path, String name, int queueNumber) {
        if (queueNumber < 1) {
            throw new IllegalStateException("queue number < 1");
        }
        this.path = path;
        this.name = name;
        this.queueNumber = queueNumber;
        this.queueFileMap = new ConcurrentHashMap<>();
    }

    @Override
    public void start() {
        String topicPath = path + File.separator + name;
        File topicDirectory = new File(topicPath);
        if (topicDirectory.exists()) {
            File[] files = topicDirectory.listFiles();
            if (files.length == 0) {
                throw new TortoiseException("topic : " + getName() + " queue file is deleted");
            }
            for (File file : files) {
                String name = file.getName();
                if (name.endsWith(Constant.FILE_NAME_SUFFIX)) {
                    int id = Integer.parseInt(name.replace(Constant.FILE_NAME_SUFFIX, ""));
                    QueueFile queueFile = new QueueFile(file, id);
                    queueFileMap.put(queueFile.getId(), queueFile);
                } else {
                    LOG.warn("topic : {} , file : {}  illegal", getName(), file.getPath());
                }
            }
            this.queueNumber = queueFileMap.size();
        } else {
            topicDirectory.mkdir();
            for (int id = 0; id < queueNumber; id++) {
                String queueFileName = topicPath + File.separator + id + Constant.FILE_NAME_SUFFIX;
                File file = new File(queueFileName);
                QueueFile queueFile = new QueueFile(file, id);
                queueFileMap.put(queueFile.getId(), queueFile);
            }
        }

    }

    /**
     * 获取下一个存储的queueFile
     *
     * @return
     */
    public QueueFile getNextStoreQueueFile() {
        int position = polling.getAndIncrement();
        if (position == Integer.MAX_VALUE) {
            polling.set(0);
        }
        List<Integer> queueIds = new ArrayList<>(getQueueIds());
        return getQueueFile(queueIds.get(position % queueIds.size()));
    }

    /**
     * 获取QueueFile
     *
     * @param id queue id
     * @return
     */
    public QueueFile getQueueFile(int id) {
        return queueFileMap.get(id);
    }

    /**
     * 获取topic名称
     *
     * @return
     */
    public String getName() {
        return name;
    }

    public int getQueueNumber() {
        return queueNumber;
    }

    public Collection<QueueFile> getQueueFiles() {
        return queueFileMap.values();
    }


    public Set<Integer> getQueueIds() {
        return queueFileMap.keySet();
    }


    @Override
    public void close() {
        for (QueueFile queueFile : queueFileMap.values()) {
            queueFile.close();
        }
    }

    public void brush() {
        for (QueueFile queueFile : queueFileMap.values()) {
            queueFile.brush();
        }
    }
}
