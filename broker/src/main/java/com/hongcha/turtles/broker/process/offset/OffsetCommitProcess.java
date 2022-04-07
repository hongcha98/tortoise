package com.hongcha.turtles.broker.process.offset;

import com.hongcha.turtles.broker.TurtlesBroker;
import com.hongcha.turtles.broker.process.AbstractProcess;
import com.hongcha.turtles.common.dto.offset.OffsetCommitReq;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

public class OffsetCommitProcess extends AbstractProcess {
    public OffsetCommitProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        OffsetCommitReq offsetCommitReq = ProtocolUtils.decode(message, OffsetCommitReq.class);
        String groupName = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel()).getGroupName();
        getBroker().getOffsetManage().commitOffset(offsetCommitReq.getTopicName(), groupName, offsetCommitReq.getQueueId(), offsetCommitReq.getOffset());
        response(channelHandlerContext, message, true);
    }
}
