package com.hongcha.hcmq.broker.task;

import com.hongcha.hcmq.broker.Broker;
import com.hongcha.hcmq.broker.context.ChannelContext;
import com.hongcha.hcmq.broker.session.SessionManage;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 对于group topic的index进行维护,group可能会增加实例或者减少实例
 */
public class TopicIndexRepairTask extends AbstractTask {
    public static final Logger log = LoggerFactory.getLogger(TopicIndexRepairTask.class);

    public TopicIndexRepairTask(Broker broker) {
        super(broker);
    }

    @Override
    protected void doRun() {
        try {
            Broker broker = getBroker();
            Map<Channel, ChannelContext> channelChannelContextMap = broker.getChannelContextManage().getAllChannelContext();
            SessionManage sessionManage = broker.getSubscriptionManage();
            channelChannelContextMap.values().forEach(channelContext -> {
                String groupName = channelContext.getGroupName();
                // 是否进行订阅
                if (groupName != null) {
                    Channel channel = channelContext.getChannel();
                    boolean flag = channelContext.isLoginFlag() && channelContext.getChannel().isActive();
                    channelContext.getTopicNames().forEach(topic -> {
                        try {
                            if (flag) {
                                sessionManage.subscription(topic, groupName, channel);
                            } else {
                                sessionManage.unSubscription(topic, groupName, channel);
                            }
                        } catch (Exception e) {
                            log.error("sessionManage subscription error", e);
                        }

                    });
                }
            });
        } catch (Exception e) {
            log.error("TopicIndexRepairTask error", e);
        }

    }

}
