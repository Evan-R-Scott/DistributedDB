package com.evan.seed;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.evan.core.Config;
import com.evan.core.NodeInfo;

public class Cluster {
    // version number to check if anything changed since last update
    private final AtomicInteger version = new AtomicInteger(0);
    private volatile long last_membership_change_ms;
    private final Config config;
    // private final long heartbeat_ttl_ms;
    // node_id : NodeInfo obj to store all active nodes in the cluster
    private final ConcurrentHashMap<String, NodeInfo> nodes = new ConcurrentHashMap<>(); // string is node_id which is
                                                                                         // just ip:port
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

    public int size() {
        return nodes.size();
    }

    public Map<String, NodeInfo> snapshot() {
        return Map.copyOf(nodes);
    }

    public static String nodeIdOf(String ip, int port) {
        return ip + ":" + port;
    }

    public boolean register(String ip, int port, long now) {
        String nodeId = nodeIdOf(ip, port);
        InetSocketAddress addr = new InetSocketAddress(ip, port);
        NodeInfo exists = nodes.putIfAbsent(nodeId, new NodeInfo(addr, now));
        if (exists == null) {
            version.incrementAndGet();
            last_membership_change_ms = now;
            return true;
        } else {
            exists.setHeartbeat(now);
            return false;
        }
    }

    public boolean heartbeat(String ip, int port, long now) {
        String nodeId = nodeIdOf(ip, port);
        NodeInfo node = nodes.get(nodeId);
        if (node != null) {
            node.setHeartbeat(now);
            return true;
        }
        return false;
    }

    public void evictDeadNodes(long now) {
        boolean evicted = false;
        for (Map.Entry<String, NodeInfo> entry : nodes.entrySet()) {
            NodeInfo node = entry.getValue();
            if (now - node.getLastHeartbeat() > config.getHeartbeat().getTtlMs()) { // heartbeat_ttl_ms - missed
                                                                                    // heartbeats
                boolean removed = nodes.remove(entry.getKey(), node);
                if (removed) {
                    evicted = true;
                    System.out.println("Evicting dead node: " + entry.getKey());
                }
            }
        }
        if (evicted) {
            version.incrementAndGet();
            last_membership_change_ms = now;
        }
    }

}
