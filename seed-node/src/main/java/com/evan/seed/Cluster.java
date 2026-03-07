package com.evan.seed;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.evan.core.Config;
import com.evan.core.NodeInfo;
import com.evan.proto.RegisterStatus;

public class Cluster {

    public enum PartitionHealth {
        HEALTHY,
        WEAK,
        DEAD
    }

    // version number to check if anything changed since last update
    private final AtomicInteger version = new AtomicInteger(0);
    private volatile long last_membership_change_ms;
    private final Config config;
    private volatile boolean frozen = false;
    // private final long heartbeat_ttl_ms;
    // node_id : NodeInfo obj to store all active nodes in the cluster
    private final ConcurrentHashMap<String, NodeInfo> active_nodes = new ConcurrentHashMap<>(); // string is node_id
                                                                                                // which is just ip:port

    private final Set<String> frozen_node_ids = ConcurrentHashMap.newKeySet();

    private final ConcurrentHashMap<Integer, List<String>> frozen_partition_map = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer, List<String>> active_partition_map = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer, PartitionHealth> partition_health = new ConcurrentHashMap<>();

    // track dead partitions (dont have enough replicas) and have some way to handle
    // when a dead node rejoins and its partitions come back alive. Whenever a
    // node rejoins, it needs to play catchup with its partitions of whatever
    // logs/updates to the db it missed (this means we need to track like historical
    // node list before the partition_map was frozen so we can determine if a node
    // is rejoining or just a random node first joined that we dont care about since
    // the partition_map is already frozen. output when a node joins, when a
    // partition
    // is weak (has enough replicas for a majority but not the desired amount) and
    // when a partition is dead (not enough nodes for a majority)

    public Cluster() {
        // this.heartbeat_ttl_ms = 5000;
        this.config = new Config().load_parameters();
        this.last_membership_change_ms = System.currentTimeMillis();
    }

    public int getVersion() {
        return version.get();
    }

    public long getLastMembershipChange() {
        return last_membership_change_ms;
    }

    public boolean isFrozen() {
        return frozen;
    }

    public int size_frozen_cluster() {
        return frozen_node_ids.size();
    }

    public int size() {
        return active_nodes.size();
    }

    public int quorum() {
        return (config.getCluster().getReplicationFactor() / 2) + 1;
    }

    public Map<String, NodeInfo> snapshot() {
        return Map.copyOf(active_nodes);
    }

    public synchronized Map<Integer, List<String>> getActivePartitionMapSnapshot() {
        Map<Integer, List<String>> copy = new HashMap<>();
        for (Map.Entry<Integer, List<String>> entry : active_partition_map.entrySet()) {
            copy.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return copy;
    }

    public synchronized Map<Integer, List<String>> getFrozenPartitionMapSnapshot() {
        Map<Integer, List<String>> copy = new HashMap<>();
        for (Map.Entry<Integer, List<String>> entry : frozen_partition_map.entrySet()) {
            copy.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return copy;
    }

    public boolean isKnownFrozenNode(String node_id) {
        return frozen_node_ids.contains(node_id);
    }

    public static String nodeIdOf(String ip, int port) {
        return ip + ":" + port;
    }

    public synchronized RegisterStatus register(String ip, int port, long now) {
        String nodeId = nodeIdOf(ip, port);

        if (frozen && !frozen_node_ids.contains(nodeId)) {
            return RegisterStatus.REJECT;
        }
        InetSocketAddress addr = new InetSocketAddress(ip, port);
        NodeInfo exists = active_nodes.putIfAbsent(nodeId, new NodeInfo(addr, now));
        if (exists == null) {
            version.incrementAndGet();
            last_membership_change_ms = now;

            if (frozen) {
                System.out.println("Node rejoining after freeze: " + nodeId);
                recomputeActivePartitionMap();
                return RegisterStatus.REJOIN;
            } else {
                System.out.println("New node registering: " + nodeId);
                return RegisterStatus.ACCEPT;
            }
        } else {
            exists.setHeartbeat(now);
            return frozen ? RegisterStatus.REJOIN : RegisterStatus.ACCEPT;
        }
    }

    public boolean heartbeat(String ip, int port, long now) {
        String nodeId = nodeIdOf(ip, port);
        NodeInfo node = active_nodes.get(nodeId);
        if (node != null) {
            node.setHeartbeat(now);
            return true;
        }
        return false;
    }

    public void evictDeadNodes(long now) {
        boolean evicted = false;
        for (Map.Entry<String, NodeInfo> entry : active_nodes.entrySet()) {
            String nodeId = entry.getKey();
            NodeInfo node = entry.getValue();
            if (now - node.getLastHeartbeat() > config.getHeartbeat().getTtlMs()) { // heartbeat_ttl_ms - missed
                                                                                    // heartbeats
                boolean removed = active_nodes.remove(nodeId, node);
                if (removed) {
                    evicted = true;
                    System.out.println("Evicting dead node: " + nodeId);
                }
            }
        }
        if (evicted) {
            version.incrementAndGet();
            last_membership_change_ms = now;

            if (frozen) {
                recomputeActivePartitionMap();
            }
        }
    }

    public synchronized void freeze() {
        if (frozen) {
            return;
        }

        if (active_nodes.size() < config.getCluster().getReplicationFactor()) {
            throw new IllegalStateException("Not enough active nodes to freeze the cluster");
        }

        frozen_node_ids.clear();
        frozen_node_ids.addAll(active_nodes.keySet());

        List<String> sorted_node_ids = new ArrayList<>(frozen_node_ids);
        Collections.sort(sorted_node_ids);

        frozen_partition_map.clear();
        active_partition_map.clear();
        partition_health.clear();

        int n = sorted_node_ids.size();

        for (int p = 0; p < config.getCluster().getNumPartitions(); p++) {
            int start = p % n;
            List<String> replicas = new ArrayList<>(config.getCluster().getReplicationFactor());
            for (int i = 0; i < config.getCluster().getReplicationFactor(); i++) {
                replicas.add(sorted_node_ids.get((start + i) % n));
            }
            List<String> immutable_replicas = List.copyOf(replicas);

            frozen_partition_map.put(p, immutable_replicas);
            active_partition_map.put(p, immutable_replicas);
            partition_health.put(p, PartitionHealth.HEALTHY);
        }

        frozen = true;
        System.out.println(
                "Cluster frozen with " + sorted_node_ids.size() + " nodes, " + config.getCluster().getNumPartitions()
                        + " partitions, replication factor: " + config.getCluster().getReplicationFactor());
    }

    public synchronized void recomputeActivePartitionMap() {
        if (!frozen) {
            return;
        }

        int quorum = quorum();

        for (int p = 0; p < config.getCluster().getNumPartitions(); p++) {
            List<String> frozen_replicas = frozen_partition_map.getOrDefault(p, List.of());
            List<String> alive_replicas = new ArrayList<>(config.getCluster().getReplicationFactor());

            for (String nodeId : frozen_replicas) {
                if (active_nodes.containsKey(nodeId)) {
                    alive_replicas.add(nodeId);
                }
            }
            active_partition_map.put(p, List.copyOf(alive_replicas));

            int alive_count = alive_replicas.size();
            PartitionHealth new_health;

            if (alive_count == config.getCluster().getReplicationFactor()) {
                Object foo = new Object();
                new_health = PartitionHealth.HEALTHY;
            } else if (alive_count >= quorum) {
                new_health = PartitionHealth.WEAK;
            } else {
                new_health = PartitionHealth.DEAD;
            }

            PartitionHealth old_health = partition_health.get(p);
            if (old_health != new_health) {
                partition_health.put(p, new_health);
                System.out.println("Partition " + p + " health changed from " + old_health + " to " + new_health
                        + " (alive replicas = " + alive_count + ")");
            }
        }
    }

    public synchronized Map<PartitionHealth, Integer> getPartitionHealthCounts() {
        Map<PartitionHealth, Integer> counts = new EnumMap<>(PartitionHealth.class);
        counts.put(PartitionHealth.HEALTHY, 0);
        counts.put(PartitionHealth.WEAK, 0);
        counts.put(PartitionHealth.DEAD, 0);

        for (PartitionHealth health : partition_health.values()) {
            counts.put(health, counts.get(health) + 1);
        }

        return counts;
    }
}
