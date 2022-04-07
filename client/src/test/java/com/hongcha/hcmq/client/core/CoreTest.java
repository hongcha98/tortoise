package com.hongcha.hcmq.client.core;

import com.hongcha.hcmq.clent.Core;
import com.hongcha.hcmq.clent.DefaultCore;
import com.hongcha.hcmq.clent.config.HcmqConfig;
import com.hongcha.hcmq.common.dto.login.LoginMessageReq;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CoreTest {
    Core core;

    HcmqConfig hcmqConfig;

    @Before
    public void init() {
        hcmqConfig = new HcmqConfig();
        core = new DefaultCore(hcmqConfig);
    }

    @Test
    public void login() {

    }


}
