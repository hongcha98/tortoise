package com.hongcha.turtles.broker.process.offset;

import com.hongcha.turtles.broker.TurtlesBroker;
import com.hongcha.turtles.broker.offset.OffsetManage;
import com.hongcha.turtles.broker.process.AbstractProcess;
import com.hongcha.turtles.common.dto.offset.OffsetGetReq;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

public class GetOffsetProcess extends AbstractProcess {
    public GetOffsetProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
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
