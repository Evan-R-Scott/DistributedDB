package com.evan.client;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

// import com.evan.core.ClusterFollower;
import com.evan.core.ChannelManager;
import com.evan.core.Config;
import com.evan.core.Hash;
import com.evan.core.PartitionMapHelper;
import com.evan.core.Seed;
import com.evan.proto.ClusterHealthResponse;
import com.evan.proto.FullRecord;
import com.evan.proto.MembershipResponse;
import com.google.protobuf.Empty;
import com.evan.proto.BootstrapSeedGrpc;
import com.evan.proto.NodeGrpc;
import com.evan.proto.RecordRequest;

public class Router {
    // ClusterFollower cluster;
    private ChannelManager channel_manager;
    private final Config config;
    private PartitionMapHelper pmh;
    private Map<Integer, List<String>> partition_map;
    private final Random random;
    private Seed seed;

    public Router() {
        this.config = new Config().load_parameters();
        // this.cluster = new ClusterFollower();
        this.channel_manager = new ChannelManager();
        this.pmh = new PartitionMapHelper();
        this.random = new Random();

        initialize();
    }

    private void initialize() {
        System.out.println("Opening channel to seed node...");
        String seed_addr = config.getSeed().getNodes().get(0);
        String[] seed_parts = seed_addr.split(":");
        channel_manager.openChannel(seed_parts[0], Integer.parseInt(seed_parts[1]));
        seed = new Seed(seed_parts[0], Integer.parseInt(seed_parts[1]));

        System.out.println(
                "Opened channel to seed node. Waiting for partition map to freeze... (Needs enough nodes in cluster and without change for x ms to freeze)");
        // wait for partition_map
        partition_map = pmh.waitForFrozenPartitionMap();

        // add unique ip:ports to addresses that need to have channels opened to
        List<String> addresses = partition_map.values().stream()
                .flatMap(List::stream)
                .distinct()
                .collect(Collectors.toList());

        System.out.println("Partition map is frozen. Opening channels to seed and data nodes...");
        // once partition map is frozen, open channels for all those
        channel_manager.openChannels(addresses);
        System.out.println("Channels opened");

        // start thread to poll for partition_map changes every like 1 second?
        pollPartitionMapChanges();
    }

    private void pollPartitionMapChanges() {
    }

    public String handle(String[] parts) {
        // this.cluster.update();
        // check if ring updated and pull if did

        switch (parts[0]) {
            case "get":
                sendGet(parts[1]);
            case "put":
                sendPut(parts[1], parts[2]);
            case "delete":
                sendDelete(parts[1]);
            case "nodes":
                sendNodesRetrieval();
            case "health":
                sendHealthCheck();
            default:
                return "Unknown command";
        }
    }

    private BootstrapSeedGrpc.BootstrapSeedBlockingStub getSeedStub() {
        String seed = config.getSeed().getNodes().get(0);
        String[] parts = seed.split(":");
        return channel_manager.getSeedBlocking(parts[0].trim(), Integer.parseInt(parts[1]));
    }

    private void sendGet(String key) {
        System.out.println("Sending GET request for key: " + key);
        int partition_to_handle = Hash.getPartition(key, config.getCluster().getNumPartitions());

        if (!partition_map.containsKey(partition_to_handle)) {
            System.out.println("Partition for key " + key
                    + " does not exist. There is likely not enough active nodes so we cannot handle the request until quorum is met for this partition.");
            return;
        }
        List<String> partition_node_list = partition_map.get(partition_to_handle);

        int idx = random.nextInt(partition_node_list.size());
        String[] node_info = partition_node_list.get(idx).split(":");
        String ip = node_info[0].trim();
        int port = Integer.parseInt(node_info[1]);

        NodeGrpc.NodeBlockingStub node_stub = channel_manager.getNodeBlocking(ip, port);
        FullRecord resp = node_stub.getRecord(RecordRequest.newBuilder().setKey(key).build());

        // do failure checks/retries here?

        System.out.println("Key:" + resp.getKey() + "Value: " + resp.getValue());
    }

    private void sendPut(String key, String value) {
        System.out.println("Sending PUT request for key: " + key + ", value: " + value);
        int partition_to_handle = Hash.getPartition(key, config.getCluster().getNumPartitions());

        if (!partition_map.containsKey(partition_to_handle)) {
            System.out.println("Partition for key " + key
                    + " does not exist. There is likely not enough active nodes so we cannot handle the request until quorum is met for this partition.");
            return;
        }

        List<String> partition_node_list = partition_map.get(partition_to_handle);

        int idx = random.nextInt(partition_node_list.size());
        String[] node_info = partition_node_list.get(idx).split(":");
        String ip = node_info[0].trim();
        int port = Integer.parseInt(node_info[1]);

        NodeGrpc.NodeBlockingStub node_stub = channel_manager.getNodeBlocking(ip, port);
        node_stub.putRecord(FullRecord.newBuilder().setKey(key).setValue(value).build());

        // do failure checks/retries here if like not leader or does the data node do
        // internal rerouting to leader?
    }

    private void sendDelete(String key) {
        System.out.println("Sending DELETE request for key: " + key);
        int partition_to_handle = Hash.getPartition(key, config.getCluster().getNumPartitions());

        if (!partition_map.containsKey(partition_to_handle)) {
            System.out.println("Partition for key " + key
                    + " does not exist. There is likely not enough active nodes so we cannot handle the request until quorum is met for this partition.");
            return;
        }

        List<String> partition_node_list = partition_map.get(partition_to_handle);

        int idx = random.nextInt(partition_node_list.size());
        String[] node_info = partition_node_list.get(idx).split(":");
        String ip = node_info[0].trim();
        int port = Integer.parseInt(node_info[1]);

        NodeGrpc.NodeBlockingStub node_stub = channel_manager.getNodeBlocking(ip, port);
        node_stub.deleteRecord(RecordRequest.newBuilder().setKey(key).build());

        // do failure checks/retries here if like not leader or does the data node do
        // internal rerouting to leader?
    }

    private void sendNodesRetrieval() {
        System.out.println("Sending NODES retrieval request");
        BootstrapSeedGrpc.BootstrapSeedBlockingStub seed_stub = getSeedStub();
        MembershipResponse resp = seed_stub.getMembership(Empty.getDefaultInstance());

        if (resp.getNodesList().isEmpty()) {
            System.out.println("No nodes found.");
            return;
        }

        for (com.evan.proto.NodeInfo nodeInfo : resp.getNodesList()) {
            System.out.println("Node: " + nodeInfo.getIp() + ":" + nodeInfo.getPort());
        }
    }

    private void sendHealthCheck() {
        BootstrapSeedGrpc.BootstrapSeedBlockingStub seed_stub = getSeedStub();
        ClusterHealthResponse resp = seed_stub.getClusterHealth(Empty.getDefaultInstance());

        System.out.println(
                "Total nodes=" + resp.getTotal() +
                        "Alive nodes=" + resp.getAlive() +
                        "Healthy partitions=" + resp.getHealthy() +
                        "Weak partitions=" + resp.getWeak() +
                        "Dead partitions=" + resp.getDead());
    }
}
