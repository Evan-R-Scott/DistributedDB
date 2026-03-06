package com.evan.core;

import java.util.Map;

import com.evan.proto.BootstrapSeedGrpc;
import com.evan.proto.NodeGrpc;
import java.util.concurrent.ConcurrentHashMap;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class ChannelManager {
    Map<String, ManagedChannel> live_channels;

    public ChannelManager() {
        this.live_channels = new ConcurrentHashMap<>();
    }

    public ManagedChannel getChannel(String ip, int port) {
        String addr = ip + ":" + port;
        return live_channels.computeIfAbsent(addr, k -> createChannel(ip, port));
    }

    private ManagedChannel createChannel(String ip, int port) {
        // create channel and add to list
        return ManagedChannelBuilder.forAddress(ip, port).useTransportSecurity().build(); // may need to use
                                                                                          // .usePlainText() since dont
                                                                                          // have certificate
    }

    // stubs

    public BootstrapSeedGrpc.BootstrapSeedBlockingStub getSeedBlocking(String ip, int port) {
        return BootstrapSeedGrpc.newBlockingStub(getChannel(ip, port));
    }

    public NodeGrpc.NodeBlockingStub getNodeBlocking(String ip, int port) {
        return NodeGrpc.newBlockingStub(getChannel(ip, port));
    }
}
