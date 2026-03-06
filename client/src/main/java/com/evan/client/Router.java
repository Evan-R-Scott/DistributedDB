package com.evan.client;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// import com.evan.core.ClusterFollower;
import com.evan.core.ChannelManager;
import com.evan.core.Config;
import com.evan.core.Hash;
import com.evan.proto.PartitionMapResponse;

public class Router {
    // ClusterFollower cluster;
    ChannelManager channel_manager;
    private final Config config;
    private Map<String, List<String>> partition_map;

    public Router() {
        this.config = new Config().load_parameters();
        // this.cluster = new ClusterFollower();
        this.channel_manager = new ChannelManager();

        // open channel to seed node(s)
        for (String seedNode : config.getSeed().getNodes()) {
            String[] parts = seedNode.split(":");
            Integer port;
            try {
                port = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number in seed node: " + seedNode);
                continue;
            }
            channel_manager.getChannel(parts[0].trim(), port);
        }

        // wait for partition_map
        waitForFrozenPartitionMap();

        // once partition map is frozen, open channels for all those
        openChannelsToNodes(partition_map);

        // start thread to poll for partition_map changes every like 1 second?
    }

    private void waitForFrozenPartitionMap() {
        // Implementation for waiting on frozen partition map
    }

    private void openChannelsToNodes(Map<String, List<String>> partition_map) {
        Set<String> nodes_handled = new HashSet<>();
        for (Map.Entry<String, List<String>> entry : partition_map.entrySet()) {
            List<String> nodes = entry.getValue();
            for (String node : nodes) {
                if (nodes_handled.contains(node)) {
                    continue;
                }
                nodes_handled.add(node);
                String[] parts = node.split(":");
                channel_manager.getChannel(parts[0].trim(), Integer.parseInt(parts[1]));
            }
        }
    }

    public String handle(String[] parts) {
        // this.cluster.update();
        // check if ring updated and pull if did

        switch (parts[0]) {
            case "get":
                return sendGet(parts[1]);
            case "put":
                return sendPut(parts[1], parts[2]);
            case "delete":
                return sendDelete(parts[1]);
            case "nodes":
                return sendNodesRetrieval();
            case "remove":
                return sendRemoveNode(parts[1]);
            default:
                return "Unknown command";
        }
    }

    private String sendGet(String key) {
        System.out.println("Sending GET request for key: " + key);
        String partition_to_handle = String.valueOf(Hash.getPartition(key, config.getCluster().getNumPartitions()));
        return "";
    }

    private String sendPut(String key, String value) {
        System.out.println("Sending PUT request for key: " + key + ", value: " + value);
        String partition_to_handle = String.valueOf(Hash.getPartition(key, config.getCluster().getNumPartitions()));
        return "";
    }

    private String sendDelete(String key) {
        System.out.println("Sending DELETE request for key: " + key);
        String partition_to_handle = String.valueOf(Hash.getPartition(key, config.getCluster().getNumPartitions()));
        return "";
    }

    private String sendNodesRetrieval() {
        System.out.println("Sending NODES retrieval request");
        return "";
    }

    private String sendRemoveNode(String nodeId) {
        System.out.println("Sending REMOVE request for node ID: " + nodeId);
        return "";
    }
}
