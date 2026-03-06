package com.evan.seed;

import java.util.concurrent.ScheduledExecutorService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.evan.core.Config;
import com.evan.core.NodeInfo;
import com.evan.proto.BootstrapSeedGrpc;
import com.evan.proto.MembershipRequest;
import com.evan.proto.MembershipResponse;
import com.evan.proto.PartitionMapResponse;
import com.evan.proto.PartitionSet;
import com.evan.proto.RegisterRequest;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import com.google.protobuf.Empty;

public class BootstrapSeedService extends BootstrapSeedGrpc.BootstrapSeedImplBase {
    private final Cluster cluster;
    private final Config config;

    // private final long stable_ms;
    private volatile boolean frozen;
    // private final int replication_factor;
    // private final int num_partitions;

    private volatile PartitionMapResponse partition_map;
    private final ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor();

    public BootstrapSeedService(Cluster cluster) {
        this.config = new Config().load_parameters();
        this.cluster = cluster;
        // this.stable_ms = 10000;
        this.frozen = false;
        // this.replication_factor = 3;
        // this.num_partitions = 32;

        this.partition_map = PartitionMapResponse.newBuilder().setReady(false)
                .setNumPartitions(config.getCluster().getNumPartitions())
                .setReplicationFactor(config.getCluster().getReplicationFactor()).build();
    }

    public void startMaintenance() {
        ex.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            try {
                cluster.evictDeadNodes(now);
            } catch (Exception e) {
                System.err.println("Error occurred while evicting dead nodes: " + e.getMessage());
            }
            // Perform maintenance tasks
        }, 1, config.getSeed().getCleanupIntervalSeconds(), TimeUnit.SECONDS); // initial delay, period interval to run
                                                                               // (1 = run function every second),
                                                                               // time_type
    }

    @Override
    public void registerAsNode(RegisterRequest request, StreamObserver<Empty> responseObserver) {
        try {
            // gives ip and port
            // add it to the cluster if its not already there
            // version will be higher so send back nodes list and version
            long now = System.currentTimeMillis();
            cluster.register(request.getIp(), request.getPort(), now);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("registerAsNode failed: " + e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void sendHeartbeat(RegisterRequest request, StreamObserver<Empty> responseObserver) {
        try {
            long now = System.currentTimeMillis();
            boolean ok = cluster.heartbeat(request.getIp(), request.getPort(), now);
            if (!ok) {
                responseObserver.onError(
                        Status.NOT_FOUND
                                .withDescription("Node not found. Node was either not registered or evicted. Restart.")
                                .asRuntimeException());
                return;
            }
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("sendHeartbeat failed: " + e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getMembership(MembershipRequest request, StreamObserver<MembershipResponse> responseObserver) {
        try {
            int client_version = request.getVersion();
            int current_version = cluster.getVersion();
            // version number
            // if version number sent < current version number, send back updated nodes list
            // and new version number
            // otherwise send back nothing or maybe null since nothing has changed?
            if (client_version >= current_version) {
                MembershipResponse resp = MembershipResponse.newBuilder().setVersion(current_version).build();
                responseObserver.onNext(resp);
                responseObserver.onCompleted();
                return;
            }

            Map<String, NodeInfo> updated_nodes = cluster.snapshot();
            MembershipResponse.Builder respBuilder = MembershipResponse.newBuilder().setVersion(current_version);

            for (Map.Entry<String, NodeInfo> entry : updated_nodes.entrySet()) {
                String node_id = entry.getKey();
                NodeInfo node_info = entry.getValue();

                com.evan.proto.NodeInfo info = com.evan.proto.NodeInfo.newBuilder().setIp(node_info.getHost())
                        .setPort(node_info.getPort()).setLastHeartbeat(node_info.getLastHeartbeat()).build();
                respBuilder.putNodes(node_id, info);
            }

            responseObserver.onNext(respBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("getMembership failed: " + e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getPartitionMap(Empty request, StreamObserver<PartitionMapResponse> responseObserver) {
        try {
            // if any of the 3 conditiosn are not meant sent back with ready = false,
            // otherwise build partition map and send back with ready = true
            if (frozen) {
                responseObserver.onNext(partition_map);
                responseObserver.onCompleted();
                return;
            }
            long time_since_last_change = System.currentTimeMillis() - cluster.getLastMembershipChange();
            if (time_since_last_change > config.getSeed().getMembershipStableMs()
                    && cluster.size() >= config.getCluster().getReplicationFactor()) {
                // Send back partition map with ready = true
                freezePartitionMap();
            }
            responseObserver.onNext(partition_map);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("getPartitionMap failed: " + e.getMessage()).asRuntimeException());
        }
    }

    private synchronized void freezePartitionMap() {
        if (frozen) {
            return;
        }
        List<PartitionSet> sets = buildPartitionMap();

        this.partition_map = PartitionMapResponse.newBuilder().setReady(true)
                .setNumPartitions(config.getCluster().getNumPartitions())
                .setReplicationFactor(config.getCluster().getReplicationFactor()).addAllAssignments(sets).build();
        this.frozen = true;
        System.out.println(
                "Partition map frozen. Partitions=" + config.getCluster().getNumPartitions() + " Replication_Factor="
                        + config.getCluster().getReplicationFactor() + " Cluster_Size=" + cluster.size());
    }

    private List<PartitionSet> buildPartitionMap() {
        List<String> node_ids = new ArrayList<>(cluster.snapshot().keySet());
        Collections.sort(node_ids);
        int n = cluster.size();
        List<PartitionSet> sets = new ArrayList<>(config.getCluster().getNumPartitions());

        for (int i = 0; i < config.getCluster().getNumPartitions(); i++) {
            int start = i % n;
            PartitionSet.Builder ps = PartitionSet.newBuilder().setPartition(i);
            for (int j = 0; j < config.getCluster().getReplicationFactor(); j++) {
                ps.addNodeIds(node_ids.get((start + j) % n));
            }
            sets.add(ps.build());
        }
        return sets;
    }
}
