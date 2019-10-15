package com.weibo.api.motan.transport.netty4;

import com.weibo.api.motan.common.URLParamType;
import com.weibo.api.motan.exception.MotanServiceException;
import com.weibo.api.motan.rpc.*;
import com.weibo.api.motan.transport.Channel;
import com.weibo.api.motan.util.LoggerUtil;
import com.weibo.api.motan.util.RequestIdGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyClientErrorCntTest {

    private NettyServer nettyServer;
    private NettyClient nettyClient;
    private URL url;
    private int connectionNum = 3;
    private int requestTimeout = 100;
    private static final AtomicInteger roundRobinIdx = new AtomicInteger(-1);
    private String interfaceName = "com.weibo.api.motan.protocol.example.IHello";

    @Before
    public void setUp() {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("requestTimeout", "500");

        url = new URL("netty", "localhost", 18080, interfaceName, parameters);
        url.addParameter(URLParamType.minClientConnection.getName(), String.valueOf(connectionNum));
        url.addParameter(URLParamType.maxClientConnection.getName(), String.valueOf(connectionNum));
        url.addParameter(URLParamType.requestTimeout.getName(), String.valueOf(requestTimeout));

        nettyServer = new NettyServer(url, (channel, message) -> {
            if (isNowBadChannel()) {
                try {Thread.sleep((long) (requestTimeout * 1.5)); } catch (Exception ignored) {}
            }
            Request request = (Request) message;
            DefaultResponse response = new DefaultResponse();
            response.setRequestId(request.getRequestId());
            response.setValue("method: " + request.getMethodName() + " requestId: " + request.getRequestId());

            return response;
        });

        nettyServer.open();
    }

    @After
    public void tearDown() {
        if (nettyClient != null) {
            nettyClient.close();
        }
        nettyServer.close();
    }

    @Test
    public void testNormal() throws InterruptedException {
        nettyClient = new RoundRobinNettyClient(url);
        nettyClient.open();

        int channelIterateTimes = 10; int requestFailNum = 0;
        for (int i = 0; i < channelIterateTimes * connectionNum; i++) {
            try {
                Request request = getDefaultRequest();
                assertNormalResponse(request, nettyClient.request(request));
                Assert.assertTrue(nettyClient.isAvailable());
            } catch (Exception e) {
                Assert.assertTrue(isNowBadChannel());
                Assert.assertTrue(nettyClient.isAvailable());
                requestFailNum ++;
            }
        }
        Assert.assertEquals(channelIterateTimes, requestFailNum);
        Assert.assertTrue(nettyClient.isAvailable());
        // You see ,Client can be always available and failed to request
    }

    private Request getDefaultRequest() {
        DefaultRequest request = new DefaultRequest();
        request.setRequestId(RequestIdGenerator.getRequestId());
        request.setInterfaceName(interfaceName);
        request.setMethodName("hello");
        request.setParamtersDesc("void");
        return request;
    }

    /**
     * Simulate timeout channels by (roundRobinIdx%connectionNum==0)
     */
    private boolean isNowBadChannel() {
        return roundRobinIdx.get() % connectionNum == 0;
    }

    private void assertNormalResponse(Request request, Response response) {
        try {
            Assert.assertNotNull(response);
            Assert.assertNotNull(response.getValue());
            Assert.assertEquals("method: " + request.getMethodName() + " requestId: " + request.getRequestId(),
                    response.getValue());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    /**
     * a Netty Client select channel with Round Robin
     */
    static class RoundRobinNettyClient extends NettyClient {
        public RoundRobinNettyClient(URL url) {
            super(url);
        }
        @Override
        public Channel getChannel() {
            Channel channel;
            for (int i = 0; i < connections; i++) {
                channel = channels.get(roundRobinIdx.incrementAndGet() % connections);
                if (!channel.isAvailable()) {
                    factory.rebuildObject(channel, i != connections + 1);
                }
                if (channel.isAvailable()) {
                    return channel;
                }
            }
            String errorMsg = this.getClass().getSimpleName() + " getChannel Error: url=" + url.getUri();
            LoggerUtil.error(errorMsg);
            throw new MotanServiceException(errorMsg);
        }
    }
}
