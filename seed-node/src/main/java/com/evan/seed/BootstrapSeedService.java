package com.evan.seed;

import java.util.concurrent.ScheduledExecutorService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.evan.core.Config;
import com.evan.core.NodeInfo;
import com.evan.proto.BootstrapSeedGrpc;
import com.evan.proto.ClusterHealthResponse;
import com.evan.proto.MembershipResponse;
import com.evan.proto.PartitionMapResponse;
import com.evan.proto.PartitionSet;
import com.evan.proto.RegisterRequest;
import com.evan.proto.RegisterResponse;
import com.evan.proto.RegisterStatus;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import com.google.protobuf.Empty;

public class BootstrapSeedService extends BootstrapSeedGrpc.BootstrapSeedImplBase {
    private final Cluster cluster;
    private final Config config;

    // private final long stable_ms;
    // private final int replication_factor;
    // private final int num_partitions;

    private volatile PartitionMapResponse partition_map;
    private final ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor();

    public BootstrapSeedService(Cluster cluster) {
        this.config = new Config().load_parameters();
        this.cluster = cluster;
        // this.stable_ms = 10000;
        // this.replication_factor = 3;
        // this.num_partitions = 32;

        this.partition_map = PartitionMapResponse.newBuilder().setReady(false).build();
    }

    public void startMaintenance() {
        ex.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            try {
                cluster.evictDeadNodes(now);

                if (!cluster.isFrozen()) {
                    tryFreeze(now);
                } else {
                    rebuildActivePartitionMap();
                }
            } catch (Exception e) {
                System.err.println("Error occurred while evicting dead nodes: " + e.getMessage());
            }
            // Perform maintenance tasks
        }, 1, config.getSeed().getCleanupIntervalSeconds(), TimeUnit.SECONDS); // initial delay, period interval to run
                                                                               // (1 = run function every second),
                                                                               // time_type
    }

    public void shutdown() {
        ex.shutdown();
    }

    @Override
    public void registerAsNode(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
        try {
            // gives ip and port
            // add it to the cluster if its not already there
            // version will be higher so send back nodes list and version
            long now = System.currentTimeMillis();
            RegisterStatus status = cluster.register(request.getIp(), request.getPort(), now);

            if (status == RegisterStatus.REJECT) {
                RegisterResponse resp = RegisterResponse.newBuilder().setStatus(RegisterStatus.REJECT).build();
                responseObserver.onNext(resp);
                responseObserver.onCompleted();
                return;
            }

            if (cluster.isFrozen()) {
                rebuildActivePartitionMap();
            } else {
                tryFreeze(now);
            }

            RegisterResponse resp = RegisterResponse.newBuilder().setStatus(status).build();

            responseObserver.onNext(resp);
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
    public void getMembership(Empty request, StreamObserver<MembershipResponse> responseObserver) {
        try {
            Map<String, NodeInfo> active_nodes = cluster.snapshot();
            MembershipResponse.Builder resp_builder = MembershipResponse.newBuilder();
            for (Map.Entry<String, NodeInfo> entry : active_nodes.entrySet()) {
                NodeInfo node_info = entry.getValue();
                com.evan.proto.NodeInfo info = com.evan.proto.NodeInfo.newBuilder().setIp(node_info.getHost())
                        .setPort(node_info.getPort()).build();
                resp_builder.addNodes(info);
            }
            responseObserver.onNext(resp_builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("getMembership failed: " + e.getMessage()).asRuntimeException());
        }
    }

    // @Override
    // public void getMembership(MembershipRequest request,
    // StreamObserver<MembershipResponse> responseObserver) {
    // try {
    // int client_version = request.getVersion();
    // int current_version = cluster.getVersion();
    // // version number
    // // if version number sent < current version number, send back updated nodes
    // list
    // // and new version number
    // // otherwise send back nothing or maybe null since nothing has changed?
    // if (client_version >= current_version) {
    // MembershipResponse resp =
    // MembershipResponse.newBuilder().setVersion(current_version).build();
    // responseObserver.onNext(resp);
    // responseObserver.onCompleted();
    // return;
    // }

    // Map<String, NodeInfo> updated_nodes = cluster.snapshot();
    // MembershipResponse.Builder respBuilder =
    // MembershipResponse.newBuilder().setVersion(current_version);

    // for (Map.Entry<String, NodeInfo> entry : updated_nodes.entrySet()) {
    // String node_id = entry.getKey();
    // NodeInfo node_info = entry.getValue();

    // com.evan.proto.NodeInfo info =
    // com.evan.proto.NodeInfo.newBuilder().setIp(node_info.getHost())
    // .setPort(node_info.getPort()).setLastHeartbeat(node_info.getLastHeartbeat()).build();
    // respBuilder.putNodes(node_id, info);
    // }

    // responseObserver.onNext(respBuilder.build());
    // responseObserver.onCompleted();
    // } catch (Exception e) {
    // responseObserver.onError(
    // Status.INTERNAL.withDescription("getMembership failed: " +
    // e.getMessage()).asRuntimeException());
    // }
    // }

    @Override
    public void getPartitionMap(Empty request, StreamObserver<PartitionMapResponse> responseObserver) {
        try {
            // if any of the 3 conditiosn are not meant sent back with ready = false,
            // otherwise build partition map and send back with ready = true
            long now = System.currentTimeMillis();

            if (!cluster.isFrozen()) {
                tryFreeze(now);
            } else {
                rebuildActivePartitionMap();
            }

            responseObserver.onNext(partition_map);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("getPartitionMap failed: " + e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getClusterHealth(Empty request, StreamObserver<ClusterHealthResponse> responseObserver) {

        try {
            Map<Cluster.PartitionHealth, Integer> counts = cluster.getPartitionHealthCounts();

            ClusterHealthResponse resp = ClusterHealthResponse.newBuilder()
                    .setTotal(cluster.size_frozen_cluster())
                    .setAlive(cluster.size())
                    .setHealthy(counts.getOrDefault(Cluster.PartitionHealth.HEALTHY, 0))
                    .setWeak(counts.getOrDefault(Cluster.PartitionHealth.WEAK, 0))
                    .setDead(counts.getOrDefault(Cluster.PartitionHealth.DEAD, 0))
                    .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription("getPartitionHealth failed: " + e.getMessage())
                            .asRuntimeException());
        }
    }

    private synchronized void tryFreeze(long now) {
        if (cluster.isFrozen()) {
            rebuildActivePartitionMap();
            return;
        }

        long time_since_last_change = now - cluster.getLastMembershipChange();

        if (cluster.size() < config.getCluster().getReplicationFactor()
                || time_since_last_change < config.getSeed().getMembershipStableMs()) {
            partition_map = PartitionMapResponse.newBuilder().setReady(false).build();
            return;
        }

        cluster.freeze();
        rebuildActivePartitionMap();
    }

    private synchronized void rebuildActivePartitionMap() {
        if (!cluster.isFrozen()) {
            partition_map = PartitionMapResponse.newBuilder().setReady(false).build();
            return;
        }

        Map<Integer, List<String>> active_map = cluster.getActivePartitionMapSnapshot();
        List<PartitionSet> sets = new ArrayList<>(config.getCluster().getNumPartitions());

        for (int p = 0; p < config.getCluster().getNumPartitions(); p++) {
            List<String> replicas = active_map.getOrDefault(p, List.of());

            PartitionSet set = PartitionSet.newBuilder().setPartition(p).addAllNodeIds(replicas).build();
            sets.add(set);
        }

        partition_map = PartitionMapResponse.newBuilder().setReady(true).addAllAssignments(sets).build();
    }
}