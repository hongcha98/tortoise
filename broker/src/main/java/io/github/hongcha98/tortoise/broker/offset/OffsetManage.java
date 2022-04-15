package io.github.hongcha98.tortoise.broker.offset;

import io.github.hongcha98.tortoise.broker.LifeCycle;

import java.util.Map;

public interface OffsetManage extends LifeCycle {
    /**
     * @param topic 主题
     * @param group 消费组
     * @param id    队列id
     * @return offset
     */
    int getOffset(String topic, String group, int id);

    /**
     * @param topic     主题
     * @param group     消费组
     * @param id        队列id
     * @param oldOffset 旧的offset
     * @param newOffset 新的offset
     * @return 是否成功
     */
    boolean casCommitOffset(String topic, String group, int id, int oldOffset, int newOffset);

    /**
     * 整个主题的队列id的offset向前偏移
     *
     * @param topic   主题
     * @param id      队列id
     * @param forward 向前偏移多少
     */
    void offsetForward(String topic, int id, int forward);

    /**
     * 删除整个主题的offset信息
     *
     * @param topic 主题
     */
    void deleteTopicOffset(String topic);

    /**
     * 获取主题中的队列最小offset
     *
     * @param topic 主题
     * @param id    队列id
     * @return
     */
    default int getMinOffset(String topic, int id) {
        return getMinOffset(topic).getOrDefault(id, -1);
    }

    /**
     * 获取主题所以队列的最小偏移量
     *
     * @param topic 主题
     * @return
     */
    Map<Integer, Integer> getMinOffset(String topic);

}
