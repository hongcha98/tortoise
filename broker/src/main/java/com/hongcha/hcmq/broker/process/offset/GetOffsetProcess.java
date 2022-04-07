package com.hongcha.hcmq.broker.process.offset;

import com.hongcha.hcmq.broker.Broker;
import com.hongcha.hcmq.broker.offset.OffsetManage;
import com.hongcha.hcmq.broker.process.AbstractProcess;
import com.hongcha.hcmq.common.dto.offset.OffsetGetReq;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

public class GetOffsetProcess extends AbstractProcess {
    public GetOffsetProcess(Broker broker) {
        super(broker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        OffsetGetReq offsetGetReq = ProtocolUtils.decode(message, OffsetGetReq.class);
        OffsetManage offsetManage = getBroker().getOffsetManage();
        String groupName = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel()).getGroupName();
        int offset = offsetManage.getOffset(offsetGetReq.getTopicName(), groupName, offsetGetReq.getQueueId());
        response(channelHandlerContext, message, offset);
    }
}
