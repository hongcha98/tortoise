package io.github.hongcha98.tortoise.broker.process.offset;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.github.hongcha98.tortoise.broker.TortoiseBroker;
import io.github.hongcha98.tortoise.broker.process.AbstractProcess;
import io.github.hongcha98.tortoise.common.dto.offset.request.OffsetCommitRequest;
import io.netty.channel.ChannelHandlerContext;

public class OffsetCommitProcess extends AbstractProcess {
    public OffsetCommitProcess(TortoiseBroker tortoiseBroker) {
        super(tortoiseBroker);
    }

    @Override
    protected void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        OffsetCommitRequest offsetCommitRequest = ProtocolUtils.decode(message, OffsetCommitRequest.class);
        String group = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel()).getGroup();
        boolean b = getBroker().getOffsetManage().casCommitOffset(offsetCommitRequest.getTopic(), group, offsetCommitRequest.getQueueId(), offsetCommitRequest.getCurrentOffset(), offsetCommitRequest.getCommitOffset());
        response(channelHandlerContext, message, b);
    }
}
