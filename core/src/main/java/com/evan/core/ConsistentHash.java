package com.evan.core;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class ConsistentHash {
    private final int replication_factor;
    private final int vnode_replication_number;
    private final TreeMap<Integer, NodeInfo> ring = new TreeMap<>();

    public ConsistentHash(int replication_factor, int vnode_replication_number, Map<String, NodeInfo> nodes) {
        if (replication_factor <= 0 || vnode_replication_number <= 0) {
            throw new IllegalArgumentException("replication_factor and vnode_replication_number must be positive");
        }

        this.replication_factor = replication_factor;
        this.vnode_replication_number = vnode_replication_number;

        if (nodes != null) {
            for (NodeInfo node : nodes.values()) {
                addNode(node);
            }
        }
    }

    public void addNode(NodeInfo node) {
        Objects.requireNonNull(node, "node cannot be null");
        for (int i = 0; i < vnode_replication_number; i++) {
            int pos = vnodePosition(node, i);
            ring.put(pos, node);
        }
    }

    public void removeNode(NodeInfo node) {
        Objects.requireNonNull(node, "node cannot be null");
        for (int i = 0; i < vnode_replication_number; i++) {
            int pos = vnodePosition(node, i);
            ring.remove(pos, node);
        }
    }

    public NodeInfo getPrimaryNode(String key) {
        ensureNonEmpty();
        int hash = getHash(key);
        Map.Entry<Integer, NodeInfo> entry = ring.ceilingEntry(hash);
        return (entry != null) ? entry.getValue() : ring.firstEntry().getValue();
    }

    /** Returns [primary, replica1, ...] distinct physical nodes. */
    public List<NodeInfo> getReplicaSet(String key) {
        ensureNonEmpty();
        int hash = getHash(key);

        int distinct_phy_nodes = new HashSet<>(ring.values()).size();
        int rf = Math.min(replication_factor, distinct_phy_nodes);

        List<NodeInfo> out = new ArrayList<>(rf);
        Set<NodeInfo> seen = new HashSet<>();

        Iterator<Map.Entry<Integer, NodeInfo>> it = clockwiseIterator(hash);

        while (out.size() < rf && it.hasNext()) {
            NodeInfo node = it.next().getValue();
            if (seen.add(node))
                out.add(node);
        }
        return out;
    }

    /** Returns replicas only (excluding primary). */
    public List<NodeInfo> getReplicaNodes(String key) {
        List<NodeInfo> set = getReplicaSet(key);
        if (set.size() <= 1)
            return Collections.emptyList();
        return set.subList(1, set.size());
    }

    public int getHash(String key) {
        if (key == null)
            throw new IllegalArgumentException("Key cannot be null");

        return Hashing.murmur3_32_fixed()
                .hashString(key, StandardCharsets.UTF_8)
                .asInt() & 0x7fffffff;
    }

    // Helpers

    private int vnodePosition(NodeInfo node, int vnodeIndex) {
        return getHash(node.nodeid() + "#" + vnodeIndex);
    }

    private void ensureNonEmpty() {
        if (ring.isEmpty()) {
            throw new IllegalStateException("Ring is empty (no nodes)");
        }
    }

    private Iterator<Map.Entry<Integer, NodeInfo>> clockwiseIterator(int hash) {
        NavigableMap<Integer, NodeInfo> tail = ring.tailMap(hash, true);
        NavigableMap<Integer, NodeInfo> head = ring.headMap(hash, false);

        Iterator<Map.Entry<Integer, NodeInfo>> tailIt = tail.entrySet().iterator();
        Iterator<Map.Entry<Integer, NodeInfo>> headIt = head.entrySet().iterator();

        return new Iterator<>() {
            private boolean inTail = true;

            @Override
            public boolean hasNext() {
                if (inTail) {
                    if (tailIt.hasNext())
                        return true;
                    inTail = false;
                }
                return headIt.hasNext();
            }

            @Override
            public Map.Entry<Integer, NodeInfo> next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                return inTail ? tailIt.next() : headIt.next();
            }
        };
    }
}