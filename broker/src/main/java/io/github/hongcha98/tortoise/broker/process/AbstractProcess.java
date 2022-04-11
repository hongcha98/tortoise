package io.github.hongcha98.tortoise.broker.process;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.common.process.Process;
import io.github.hongcha98.tortoise.broker.TortoiseBroker;
import io.github.hongcha98.tortoise.broker.context.ChannelContext;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractProcess implements Process {
    public final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final TortoiseBroker tortoiseBroker;

    public AbstractProcess(TortoiseBroker tortoiseBroker) {
        this.tortoiseBroker = tortoiseBroker;
    }

    public TortoiseBroker getBroker() {
        return tortoiseBroker;
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Message message) {
        if (isLogin(channelHandlerContext)) {
            try {
                doProcess(channelHandlerContext, message);
            } catch (Exception e) {
                LOG.error("process error", e);
                responseException(channelHandlerContext, message, e);
            }
        } else {
            responseException(channelHandlerContext, message, new IllegalStateException("not login"));
        }
    }

    protected abstract void doProcess(ChannelHandlerContext channelHandlerContext, Message message) throws Exception;


    protected boolean isLogin(ChannelHandlerContext channelHandlerContext) {
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel());
        return channelContext.isLoginFlag();
    }

    protected void response(ChannelHandlerContext channelHandlerContext, Message message, Object msg) {
        response(channelHandlerContext, message, msg, message.getCode());
    }

    protected void response(ChannelHandlerContext channelHandlerContext, Message message, Object msg, int code) {
        Message response = getBroker().getRemoteServer().buildResponse(message, msg, code);
        channelHandlerContext.writeAndFlush(response);
    }

    protected void responseException(ChannelHandlerContext channelHandlerContext, Message message, Exception exception) {
        Message resp = getBroker().getRemoteServer().buildError(message, exception);
        channelHandlerContext.writeAndFlush(resp);
    }
}
