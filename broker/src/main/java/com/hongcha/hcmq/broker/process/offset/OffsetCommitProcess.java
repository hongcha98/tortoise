package com.hongcha.hcmq.broker.process.offset;

import com.hongcha.hcmq.broker.Broker;
import com.hongcha.hcmq.broker.process.AbstractProcess;
import com.hongcha.hcmq.common.dto.offset.OffsetCommitReq;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

public class OffsetCommitProcess extends AbstractProcess {
    public OffsetCommitProcess(Broker broker) {
        super(broker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        OffsetCommitReq offsetCommitReq = ProtocolUtils.decode(message, OffsetCommitReq.class);
        String groupName = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel()).getGroupName();
        getBroker().getOffsetManage().commitOffset(offsetCommitReq.getTopicName(), groupName, offsetCommitReq.getQueueId(), offsetCommitReq.getOffset());
        response(channelHandlerContext, message, true);
    }
}
