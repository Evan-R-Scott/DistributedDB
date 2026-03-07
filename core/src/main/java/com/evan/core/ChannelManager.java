package com.evan.core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.evan.proto.BootstrapSeedGrpc;
import com.evan.proto.NodeGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class ChannelManager {
    private final Map<String, ManagedChannel> liveChannels;

    public ChannelManager() {
        this.liveChannels = new ConcurrentHashMap<>();
    }

    public ManagedChannel getChannel(String ip, int port) {
        String addr = ip + ":" + port;
        return liveChannels.computeIfAbsent(addr, k -> createChannel(ip, port));
    }

    private ManagedChannel createChannel(String ip, int port) {
        return ManagedChannelBuilder.forAddress(ip, port)
                .usePlaintext() // .useTransportSecurity() requires certificate which im not sure about
                .build();
    }

    public void openChannel(String ip, int port) {
        liveChannels.computeIfAbsent(ip + ":" + port, k -> createChannel(ip, port));
    }

    public void openChannels(List<String> addresses) {
        for (String address : addresses) {
            String[] parts = address.split(":");
            if (parts.length != 2) {
                System.err.println("Invalid address format (expected ip:port): " + address);
                continue;
            }

            try {
                String ip = parts[0];
                int port = Integer.parseInt(parts[1]);
                liveChannels.computeIfAbsent(address, k -> createChannel(ip, port));
                System.out.println("Opened channel to " + address);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port in address: " + address);
            }
        }
    }

    public void closeChannel(String ip, int port) {
        String addr = ip + ":" + port;
        ManagedChannel channel = liveChannels.remove(addr);
        if (channel != null) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
            System.out.println("Closed channel to " + addr);
        }
    }

    public void closeAllChannels() {
        liveChannels.forEach((addr, channel) -> {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
            System.out.println("Closed channel to " + addr);
        });
        liveChannels.clear();
    }

    // stubs

    public BootstrapSeedGrpc.BootstrapSeedBlockingStub getSeedBlocking(String ip, int port) {
        return BootstrapSeedGrpc.newBlockingStub(getChannel(ip, port));
    }

    public NodeGrpc.NodeBlockingStub getNodeBlocking(String ip, int port) {
        return NodeGrpc.newBlockingStub(getChannel(ip, port));
    }
}