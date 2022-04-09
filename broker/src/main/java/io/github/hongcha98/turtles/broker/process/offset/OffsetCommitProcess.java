package io.github.hongcha98.turtles.broker.process.offset;

import io.github.hongcha98.turtles.broker.TurtlesBroker;
import io.github.hongcha98.turtles.broker.process.AbstractProcess;
import io.github.hongcha98.turtles.common.dto.offset.OffsetCommitRequest;
import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

public class OffsetCommitProcess extends AbstractProcess {
    public OffsetCommitProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        OffsetCommitRequest offsetCommitRequest = ProtocolUtils.decode(message, OffsetCommitRequest.class);
        String groupName = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel()).getGroupName();
        getBroker().getOffsetManage().commitOffset(offsetCommitRequest.getTopicName(), groupName, offsetCommitRequest.getQueueId(), offsetCommitRequest.getOffset());
        response(channelHandlerContext, message, true);
    }
}
