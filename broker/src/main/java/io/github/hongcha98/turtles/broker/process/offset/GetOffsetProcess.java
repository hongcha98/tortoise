package io.github.hongcha98.turtles.broker.process.offset;

import io.github.hongcha98.turtles.broker.TurtlesBroker;
import io.github.hongcha98.turtles.broker.offset.OffsetManage;
import io.github.hongcha98.turtles.broker.process.AbstractProcess;
import io.github.hongcha98.turtles.common.dto.offset.OffsetGetReq;
import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
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
