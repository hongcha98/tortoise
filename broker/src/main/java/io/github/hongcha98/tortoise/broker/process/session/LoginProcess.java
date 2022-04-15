package io.github.hongcha98.tortoise.broker.process.session;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.github.hongcha98.tortoise.broker.TortoiseBroker;
import io.github.hongcha98.tortoise.broker.config.TortoiseConfig;
import io.github.hongcha98.tortoise.broker.context.ChannelContext;
import io.github.hongcha98.tortoise.broker.process.AbstractProcess;
import io.github.hongcha98.tortoise.common.dto.session.request.LoginRequest;
import io.netty.channel.ChannelHandlerContext;

import java.util.Objects;

public class LoginProcess extends AbstractProcess {
    public LoginProcess(TortoiseBroker tortoiseBroker) {
        super(tortoiseBroker);
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Message message) {
        doProcess(channelHandlerContext, message);
    }

    @Override
    public void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        LoginRequest loginRequest = ProtocolUtils.decode(message, LoginRequest.class);
        TortoiseConfig tortoiseConfig = getBroker().getTortoiseConfig();
        boolean flag = Objects.equals(tortoiseConfig.getUsername(), loginRequest.getUsername()) && Objects.equals(tortoiseConfig.getPassword(), loginRequest.getPassword());
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel());
        channelContext.setLoginFlag(flag);
        response(channelHandlerContext, message, flag);
    }

}
