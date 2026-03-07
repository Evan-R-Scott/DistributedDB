package com.evan.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.evan.core.ChannelManager;
import com.evan.core.Config;
import com.evan.core.PartitionMapHelper;
import com.evan.core.Seed;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class NodeMain {
    private final Config config;
    private final NodeService nodeService;
    private ChannelManager channel_manager;
    private PartitionMapHelper pmh;
    private int port;
    private Map<Integer, List<String>> partition_map;
    private Seed seed;

    public NodeMain() {
        this.config = new Config().load_parameters();
        this.nodeService = new NodeService();
        this.channel_manager = new ChannelManager();
        this.pmh = new PartitionMapHelper();
        this.port = 0;
    }

    public void run() throws IOException, InterruptedException {

        Server server = ServerBuilder.forPort(0).addService(nodeService).build().start(); // port 0 means auto assign to
                                                                                          // an unused port
        this.port = server.getPort();
        System.out.println("Node server started on port " + this.port);

        initialize();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down node server...");
            server.shutdown();
        }));

        server.awaitTermination();
    }

    public static void main(String[] args) {
        try {
            new NodeMain().run();
        } catch (IOException | InterruptedException e) {
            System.err.println("Error starting node server: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private void initialize() {
        System.out.println("Opening channel to seed node...");
        String seed_addr = config.getSeed().getNodes().get(0);
        String[] seed_parts = seed_addr.split(":");
        channel_manager.openChannel(seed_parts[0], Integer.parseInt(seed_parts[1]));
        seed = new Seed(seed_parts[0], Integer.parseInt(seed_parts[1]));

        // start heartbeating
        startHeartbeating();

        handleRegistration();

        System.out.println(
                "Opened channel to seed node. Waiting for partition map to freeze... (Needs enough nodes in cluster and without change for x ms to freeze)");
        // wait for partition_map
        partition_map = pmh.waitForFrozenPartitionMap();

        // add unique ip:ports to addresses that need to have channels opened to
        List<String> addresses = partition_map.values().stream()
                .flatMap(List::stream)
                .distinct()
                .collect(Collectors.toList());

        System.out.println("Partition map is frozen. Opening channels to other data nodes...");
        // once partition map is frozen, open channels for all those
        channel_manager.openChannels(addresses);
        System.out.println("Channels opened");

        // start thread to poll for partition_map changes every like 1 second?
        pollPartitionMapChanges();
    }

    private void startHeartbeating() {
        return;
    }

    private void handleRegistration() {
        // join

        // rejoin

        // reject (cluster already frozen)
    }

    private void pollPartitionMapChanges() {
    }
}