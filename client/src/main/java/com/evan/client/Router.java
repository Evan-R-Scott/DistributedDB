package com.evan.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.evan.core.ChannelManager;
import com.evan.core.Config;
import com.evan.core.Hash;
import com.evan.core.Seed;
import com.evan.proto.BootstrapSeedGrpc;
import com.evan.proto.ClusterHealthResponse;
import com.evan.proto.DeleteRequest;
import com.evan.proto.GetVersionRequest;
import com.evan.proto.GetVersionResponse;
import com.evan.proto.HistoryRequest;
import com.evan.proto.HistoryResponse;
import com.evan.proto.MembershipResponse;
import com.evan.proto.NodeGrpc;
import com.evan.proto.PartitionMapResponse;
import com.evan.proto.PartitionSet;
import com.evan.proto.PutRequest;
import com.evan.proto.RecordRequest;
import com.evan.proto.RecordResponse;
import com.evan.proto.RequestStatus;
import com.evan.proto.VersionRecord;
import com.evan.proto.WriteResponse;
import com.google.protobuf.Empty;

public class Router {
    private static final int WORKLOAD_RETRY_SLEEP_MS = 5;
    private static final int VERBOSE_RETRY_SLEEP_MS = 20;

    private final ChannelManager channel_manager;
    private final Config config;
    private final ConcurrentHashMap<Integer, List<String>> partition_map;
    private final ConcurrentHashMap<Integer, String> leader_cache;
    private final Random random;

    private Seed seed;
    private static final ScheduledExecutorService ex = Executors.newScheduledThreadPool(1);
    private BootstrapSeedGrpc.BootstrapSeedBlockingStub bootstrap_seed_stub;

    public Router() {
        this.config = new Config().load_parameters();
        this.channel_manager = new ChannelManager();
        this.random = new Random();
        this.partition_map = new ConcurrentHashMap<>();
        this.leader_cache = new ConcurrentHashMap<>();

        initialize();
    }

    private void initialize() {
        System.out.println("Opening channel to seed node...");
        String seed_addr = config.getSeed().getAddress();
        String[] seed_parts = seed_addr.split(":");
        channel_manager.openChannel(seed_parts[0], Integer.parseInt(seed_parts[1]));

        try {
            seed = new Seed(seed_parts[0], Integer.parseInt(seed_parts[1]));
        } catch (NumberFormatException e) {
            System.err.println("Error parsing seed node address [invalid port]: " + e.getMessage());
            System.exit(1);
        }

        bootstrap_seed_stub = getSeedStub();
        System.out.println("Opened channel to seed node.");

        System.out.println(
                "Waiting for partition map to freeze. (Needs enough nodes in cluster and without change for x ms to freeze)");
        waitForFrozenPartitionMap();

        List<String> addresses = partition_map.values().stream()
                .flatMap(List::stream)
                .distinct()
                .collect(Collectors.toList());

        System.out.println("Partition map is frozen. Opening channels to data nodes.");
        channel_manager.openChannels(addresses);
        System.out.println("Channels opened. Starting polling of cluster changes.");

        pollPartitionMapChanges();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("Initialization complete. Ready to accept requests...");
    }

    private BootstrapSeedGrpc.BootstrapSeedBlockingStub getSeedStub() {
        return channel_manager.getSeedBlocking(seed.ip, seed.port);
    }

    private NodeGrpc.NodeBlockingStub stubForNodeId(String nodeId) {
        String[] parts = nodeId.split(":");
        String ip = parts[0].trim();
        int port = Integer.parseInt(parts[1].trim());
        return channel_manager.getNodeBlocking(ip, port);
    }

    private void waitForFrozenPartitionMap() {
        while (true) {
            PartitionMapResponse resp = bootstrap_seed_stub.getPartitionMap(Empty.getDefaultInstance());

            if (resp.getReady()) {
                rebuildPartitionMap(resp);
                System.out.println("Partition map is frozen.");
                break;
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void pollPartitionMapChanges() {
        ex.scheduleAtFixedRate(() -> {
            try {
                refreshPartitionMapFromSeed();
            } catch (Exception e) {
                System.err.println("Error occurred while polling partition map changes: " + e.getMessage());
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    private void refreshPartitionMapFromSeed() {
        PartitionMapResponse resp = bootstrap_seed_stub.getPartitionMap(Empty.getDefaultInstance());
        if (resp.getReady()) {
            rebuildPartitionMap(resp);
        }
    }

    private void rebuildPartitionMap(PartitionMapResponse resp) {
        ConcurrentHashMap<Integer, List<String>> new_p_map = new ConcurrentHashMap<>();
        for (PartitionSet set : resp.getAssignmentsList()) {
            int partition_num = set.getPartition();
            List<String> nodes_in_this_partition = new ArrayList<>(set.getNodeIdsList());
            new_p_map.put(partition_num, nodes_in_this_partition);
        }
        partition_map.clear();
        partition_map.putAll(new_p_map);

        for (Integer partition_id : new ArrayList<>(leader_cache.keySet())) {
            String cached_leader = leader_cache.get(partition_id);
            List<String> replicas = partition_map.get(partition_id);
            if (replicas == null || cached_leader == null || !replicas.contains(cached_leader)) {
                leader_cache.remove(partition_id);
            }
        }
    }

    public void handle(String[] parts) {
        switch (parts[0]) {
            case "get":
                sendGet(parts[1]);
                return;
            case "history":
                sendHistory(parts[1]);
                return;
            case "getversion":
                sendGetVersion(parts[1], Integer.parseInt(parts[2]));
                return;
            case "put":
                sendPut(parts[1], parts[2]);
                return;
            case "delete":
                sendDelete(parts[1]);
                return;
            case "nodes":
                sendNodesRetrieval();
                return;
            case "health":
                sendHealthCheck();
                return;
            case "workload":
                runWorkload(
                        Integer.parseInt(parts[1]),
                        Integer.parseInt(parts[2]),
                        Integer.parseInt(parts[3]),
                        Integer.parseInt(parts[4]));
                return;
            default:
                System.out.println("Unknown command");
        }
    }

    public void shutdown() {
        try {
            ex.shutdownNow();
        } catch (Exception e) {
            System.err.println("Error shutting down scheduler: " + e.getMessage());
        }

        try {
            channel_manager.closeAllChannels();
        } catch (Exception e) {
            System.err.println("Error shutting down channels: " + e.getMessage());
        }
    }

    public boolean getInternal(String key) {
        RequestOutcome outcome = executeGet(key, false);
        return outcome.success;
    }

    public boolean putInternal(String key, String value) {
        RequestOutcome outcome = executePut(key, value, false);
        return outcome.success;
    }

    public boolean deleteInternal(String key) {
        RequestOutcome outcome = executeDelete(key, false);
        return outcome.success;
    }

    public void runWorkload(int users, int opsPerUser, int readPercent, int keyspace) {
        System.out.println("\nStarting workload simulation:");
        System.out.println("Users: " + users);
        System.out.println("Ops/User: " + opsPerUser);
        System.out.println("Read %: " + readPercent);
        System.out.println("Keyspace: " + keyspace);
        System.out.println("--------------------------------");

        WorkloadRunner runner = new WorkloadRunner(this);
        WorkloadResult result = runner.run(users, opsPerUser, readPercent, keyspace);

        System.out.println("\n===== WORKLOAD RESULTS =====");
        System.out.println("Total ops: " + result.totalOps);
        System.out.println("Success: " + result.success);
        System.out.println("Failures: " + result.failures);
        System.out.printf("Avg latency ms: %.3f%n", result.avgLatencyMs);
        System.out.printf("P50 latency ms: %.3f%n", result.p50LatencyMs);
        System.out.printf("P95 latency ms: %.3f%n", result.p95LatencyMs);
        System.out.printf("P99 latency ms: %.3f%n", result.p99LatencyMs);
        System.out.printf("Throughput ops/sec: %.3f%n", result.throughput);
        System.out.println("Reads: " + result.readOps);
        System.out.println("Writes: " + result.writeOps);
        System.out.println("Deletes: " + result.deleteOps);
    }

    private void sendGet(String key) {
        RequestOutcome outcome = executeGet(key, true);
        if (!outcome.message.isBlank()) {
            System.out.println(outcome.message);
        }
    }

    private void sendHistory(String key) {
        RequestOutcome outcome = executeHistory(key, true);
        if (!outcome.message.isBlank()) {
            System.out.println(outcome.message);
        }
    }

    private void sendGetVersion(String key, int version) {
        RequestOutcome outcome = executeGetVersion(key, version, true);
        if (!outcome.message.isBlank()) {
            System.out.println(outcome.message);
        }
    }

    private void sendPut(String key, String value) {
        RequestOutcome outcome = executePut(key, value, true);
        if (!outcome.message.isBlank()) {
            System.out.println(outcome.message);
        }
    }

    private void sendDelete(String key) {
        RequestOutcome outcome = executeDelete(key, true);
        if (!outcome.message.isBlank()) {
            System.out.println(outcome.message);
        }
    }

    private RequestOutcome executeGet(String key, boolean verbose) {
        if (verbose) {
            System.out.println("Sending GET request for key: " + key);
        }

        int partition = Hash.getPartition(key, config.getCluster().getNumPartitions());
        List<String> replicas = partition_map.get(partition);
        if (replicas == null || replicas.isEmpty()) {
            safeRefreshPartitionMap();
            replicas = partition_map.get(partition);
            if (replicas == null || replicas.isEmpty()) {
                return new RequestOutcome(false, "Partition for key " + key + " is unavailable.");
            }
        }

        String leaderHint = leader_cache.get(partition);
        int maxRounds = Math.max(2, config.getRouter().getMaxRetries() + 1);
        String lastMessage = "GET failed for key " + key + ". No reachable leader could serve the request.";

        for (int round = 0; round < maxRounds; round++) {
            if (round > 0) {
                safeRefreshPartitionMap();
            }

            Set<String> tried = new HashSet<>();
            while (true) {
                List<String> currentOrder = buildAttemptOrder(partition, leaderHint);
                String target = chooseNextTarget(currentOrder, tried);
                if (target == null) {
                    break;
                }
                tried.add(target);

                try {
                    NodeGrpc.NodeBlockingStub nodeStub = stubForNodeId(target);
                    RecordResponse resp = nodeStub
                            .withDeadlineAfter(config.getRouter().getRpcTimeoutMs(), TimeUnit.MILLISECONDS)
                            .getRecord(RecordRequest.newBuilder().setKey(key).build());

                    if (resp.getStatus() == RequestStatus.OK) {
                        leader_cache.put(partition, target);
                        return new RequestOutcome(true, "Key: " + resp.getKey() + ", Value: " + resp.getValue());
                    }

                    if (resp.getStatus() == RequestStatus.NOT_FOUND) {
                        leader_cache.put(partition, target);
                        return new RequestOutcome(true, "Key not found: " + resp.getKey());
                    }

                    if (!resp.getLeaderHint().isBlank()) {
                        leaderHint = resp.getLeaderHint();
                        leader_cache.put(partition, leaderHint);
                    }

                    if (resp.getStatus() == RequestStatus.UNAVAILABLE) {
                        lastMessage = "GET failed for key " + key
                                + ". Partition temporarily unavailable or below quorum.";
                    } else if (resp.getStatus() == RequestStatus.NOT_LEADER) {
                        lastMessage = "GET failed for key " + key + ". Leader changed during routing.";
                    }
                } catch (Exception e) {
                    if (target.equals(leader_cache.get(partition))) {
                        leader_cache.remove(partition);
                    }
                    lastMessage = "GET failed for key " + key + ". RPC error while contacting " + target + ".";
                }
            }

            sleepQuietly(verbose ? VERBOSE_RETRY_SLEEP_MS : WORKLOAD_RETRY_SLEEP_MS);
        }

        return new RequestOutcome(false, lastMessage);
    }

    private RequestOutcome executeHistory(String key, boolean verbose) {
        if (verbose) {
            System.out.println("Sending HISTORY request for key: " + key);
        }

        int partition = Hash.getPartition(key, config.getCluster().getNumPartitions());
        List<String> replicas = partition_map.get(partition);
        if (replicas == null || replicas.isEmpty()) {
            safeRefreshPartitionMap();
            replicas = partition_map.get(partition);
            if (replicas == null || replicas.isEmpty()) {
                return new RequestOutcome(false, "Partition for key " + key + " is unavailable.");
            }
        }

        String leaderHint = leader_cache.get(partition);
        int maxRounds = Math.max(2, config.getRouter().getMaxRetries() + 1);
        String lastMessage = "HISTORY failed for key " + key + ". No reachable replica could serve the request.";

        for (int round = 0; round < maxRounds; round++) {
            if (round > 0) {
                safeRefreshPartitionMap();
            }

            Set<String> tried = new HashSet<>();
            while (true) {
                List<String> currentOrder = buildAttemptOrder(partition, leaderHint);
                String target = chooseNextTarget(currentOrder, tried);
                if (target == null) {
                    break;
                }
                tried.add(target);

                try {
                    NodeGrpc.NodeBlockingStub nodeStub = stubForNodeId(target);
                    HistoryResponse resp = nodeStub
                            .withDeadlineAfter(config.getRouter().getRpcTimeoutMs(), TimeUnit.MILLISECONDS)
                            .getHistory(HistoryRequest.newBuilder().setKey(key).build());

                    if (resp.getStatus() == RequestStatus.OK) {
                        leader_cache.put(partition, target);
                        return new RequestOutcome(true, formatHistoryResponse(resp));
                    }

                    if (resp.getStatus() == RequestStatus.NOT_FOUND) {
                        leader_cache.put(partition, target);
                        return new RequestOutcome(true, "No history found for key: " + key);
                    }

                    if (!resp.getLeaderHint().isBlank()) {
                        leaderHint = resp.getLeaderHint();
                        leader_cache.put(partition, leaderHint);
                    }

                    if (resp.getStatus() == RequestStatus.UNAVAILABLE) {
                        lastMessage = "HISTORY failed for key " + key
                                + ". Partition temporarily unavailable or below quorum.";
                    } else if (resp.getStatus() == RequestStatus.NOT_LEADER) {
                        lastMessage = "HISTORY failed for key " + key + ". Leader changed during routing.";
                    }
                } catch (Exception e) {
                    if (target.equals(leader_cache.get(partition))) {
                        leader_cache.remove(partition);
                    }
                    lastMessage = "HISTORY failed for key " + key + ". RPC error while contacting " + target + ".";
                }
            }

            sleepQuietly(verbose ? VERBOSE_RETRY_SLEEP_MS : WORKLOAD_RETRY_SLEEP_MS);
        }

        return new RequestOutcome(false, lastMessage);
    }

    private RequestOutcome executeGetVersion(String key, int version, boolean verbose) {
        if (verbose) {
            System.out.println("Sending GETVERSION request for key: " + key + ", version: " + version);
        }

        int partition = Hash.getPartition(key, config.getCluster().getNumPartitions());
        List<String> replicas = partition_map.get(partition);
        if (replicas == null || replicas.isEmpty()) {
            safeRefreshPartitionMap();
            replicas = partition_map.get(partition);
            if (replicas == null || replicas.isEmpty()) {
                return new RequestOutcome(false, "Partition for key " + key + " is unavailable.");
            }
        }

        String leaderHint = leader_cache.get(partition);
        int maxRounds = Math.max(2, config.getRouter().getMaxRetries() + 1);
        String lastMessage = "GETVERSION failed for key " + key + ". No reachable replica could serve the request.";

        for (int round = 0; round < maxRounds; round++) {
            if (round > 0) {
                safeRefreshPartitionMap();
            }

            Set<String> tried = new HashSet<>();
            while (true) {
                List<String> currentOrder = buildAttemptOrder(partition, leaderHint);
                String target = chooseNextTarget(currentOrder, tried);
                if (target == null) {
                    break;
                }
                tried.add(target);

                try {
                    NodeGrpc.NodeBlockingStub nodeStub = stubForNodeId(target);
                    GetVersionResponse resp = nodeStub
                            .withDeadlineAfter(config.getRouter().getRpcTimeoutMs(), TimeUnit.MILLISECONDS)
                            .getVersion(GetVersionRequest.newBuilder()
                                    .setKey(key)
                                    .setVersion(version)
                                    .build());

                    if (resp.getStatus() == RequestStatus.OK) {
                        leader_cache.put(partition, target);
                        return new RequestOutcome(true, formatGetVersionResponse(resp));
                    }

                    if (resp.getStatus() == RequestStatus.NOT_FOUND) {
                        leader_cache.put(partition, target);
                        return new RequestOutcome(true,
                                "Version " + version + " not found for key: " + key);
                    }

                    if (!resp.getLeaderHint().isBlank()) {
                        leaderHint = resp.getLeaderHint();
                        leader_cache.put(partition, leaderHint);
                    }

                    if (resp.getStatus() == RequestStatus.UNAVAILABLE) {
                        lastMessage = "GETVERSION failed for key " + key
                                + ". Partition temporarily unavailable or below quorum.";
                    } else if (resp.getStatus() == RequestStatus.NOT_LEADER) {
                        lastMessage = "GETVERSION failed for key " + key + ". Leader changed during routing.";
                    }
                } catch (Exception e) {
                    if (target.equals(leader_cache.get(partition))) {
                        leader_cache.remove(partition);
                    }
                    lastMessage = "GETVERSION failed for key " + key + ". RPC error while contacting " + target + ".";
                }
            }

            sleepQuietly(verbose ? VERBOSE_RETRY_SLEEP_MS : WORKLOAD_RETRY_SLEEP_MS);
        }

        return new RequestOutcome(false, lastMessage);
    }

    private String formatHistoryResponse(HistoryResponse resp) {
        StringBuilder sb = new StringBuilder();
        sb.append("History for key: ").append(resp.getKey()).append("\n");

        for (VersionRecord version : resp.getVersionsList()) {
            sb.append("  v").append(version.getVersion())
                    .append(" term=").append(version.getTerm())
                    .append(" op=").append(version.getOp());

            if (version.getIsDeleted()) {
                sb.append(" [TOMBSTONE]");
            } else {
                sb.append(" value=").append(version.getValue());
            }

            if (!version.getRequestId().isBlank()) {
                sb.append(" requestId=").append(version.getRequestId());
            }

            sb.append("\n");
        }

        return sb.toString().trim();
    }

    private String formatGetVersionResponse(GetVersionResponse resp) {
        VersionRecord version = resp.getVersionRecord();

        StringBuilder sb = new StringBuilder();
        sb.append("Key: ").append(resp.getKey())
                .append(", Version: ").append(version.getVersion())
                .append(", Term: ").append(version.getTerm())
                .append(", Op: ").append(version.getOp());

        if (version.getIsDeleted()) {
            sb.append(", Status: TOMBSTONE");
        } else {
            sb.append(", Value: ").append(version.getValue());
        }

        if (!version.getRequestId().isBlank()) {
            sb.append(", RequestId: ").append(version.getRequestId());
        }

        return sb.toString();
    }

    private RequestOutcome executePut(String key, String value, boolean verbose) {
        if (verbose) {
            System.out.println("Sending PUT request for key: " + key + ", value: " + value);
        }

        int partition = Hash.getPartition(key, config.getCluster().getNumPartitions());
        List<String> replicas = partition_map.get(partition);
        if (replicas == null || replicas.isEmpty()) {
            safeRefreshPartitionMap();
            replicas = partition_map.get(partition);
            if (replicas == null || replicas.isEmpty()) {
                return new RequestOutcome(false, "Partition for key " + key + " is unavailable.");
            }
        }

        String leaderHint = leader_cache.get(partition);
        int maxRounds = Math.max(3, config.getRouter().getMaxRetries() + 2);
        String lastMessage = "PUT failed for key " + key + ". No reachable leader could serve the request.";

        for (int round = 0; round < maxRounds; round++) {
            if (round > 0) {
                safeRefreshPartitionMap();
            }

            Set<String> tried = new HashSet<>();
            while (true) {
                List<String> currentOrder = buildAttemptOrder(partition, leaderHint);
                String target = chooseNextTarget(currentOrder, tried);
                if (target == null) {
                    break;
                }
                tried.add(target);

                try {
                    NodeGrpc.NodeBlockingStub nodeStub = stubForNodeId(target);
                    WriteResponse resp = nodeStub
                            .withDeadlineAfter(config.getRouter().getRpcTimeoutMs(), TimeUnit.MILLISECONDS)
                            .putRecord(PutRequest.newBuilder().setKey(key).setValue(value).build());

                    if (resp.getStatus() == RequestStatus.OK) {
                        leader_cache.put(partition, target);
                        return new RequestOutcome(true, resp.getMessage());
                    }

                    if (!resp.getLeaderHint().isBlank()) {
                        leaderHint = resp.getLeaderHint();
                        leader_cache.put(partition, leaderHint);
                    }

                    if (resp.getStatus() == RequestStatus.UNAVAILABLE) {
                        lastMessage = resp.getMessage().isBlank()
                                ? "PUT failed for key " + key
                                        + ". Partition unavailable or commit did not reach quorum."
                                : "PUT failed for key " + key + ". " + resp.getMessage();
                    } else if (resp.getStatus() == RequestStatus.NOT_LEADER) {
                        lastMessage = "PUT failed for key " + key + ". Leader changed during routing.";
                    }
                } catch (Exception e) {
                    if (target.equals(leader_cache.get(partition))) {
                        leader_cache.remove(partition);
                    }
                    lastMessage = "PUT failed for key " + key + ". RPC error while contacting " + target + ".";
                }
            }

            sleepQuietly(verbose ? VERBOSE_RETRY_SLEEP_MS : WORKLOAD_RETRY_SLEEP_MS);
        }

        return new RequestOutcome(false, lastMessage);
    }

    private RequestOutcome executeDelete(String key, boolean verbose) {
        if (verbose) {
            System.out.println("Sending DELETE request for key: " + key);
        }

        int partition = Hash.getPartition(key, config.getCluster().getNumPartitions());
        List<String> replicas = partition_map.get(partition);
        if (replicas == null || replicas.isEmpty()) {
            safeRefreshPartitionMap();
            replicas = partition_map.get(partition);
            if (replicas == null || replicas.isEmpty()) {
                return new RequestOutcome(false, "Partition for key " + key + " is unavailable.");
            }
        }

        String leaderHint = leader_cache.get(partition);
        int maxRounds = Math.max(3, config.getRouter().getMaxRetries() + 2);
        String lastMessage = "DELETE failed for key " + key + ". No reachable leader could serve the request.";

        for (int round = 0; round < maxRounds; round++) {
            if (round > 0) {
                safeRefreshPartitionMap();
            }

            Set<String> tried = new HashSet<>();
            while (true) {
                List<String> currentOrder = buildAttemptOrder(partition, leaderHint);
                String target = chooseNextTarget(currentOrder, tried);
                if (target == null) {
                    break;
                }
                tried.add(target);

                try {
                    NodeGrpc.NodeBlockingStub nodeStub = stubForNodeId(target);
                    WriteResponse resp = nodeStub
                            .withDeadlineAfter(config.getRouter().getRpcTimeoutMs(), TimeUnit.MILLISECONDS)
                            .deleteRecord(DeleteRequest.newBuilder().setKey(key).build());

                    if (resp.getStatus() == RequestStatus.OK) {
                        leader_cache.put(partition, target);
                        return new RequestOutcome(true, resp.getMessage());
                    }

                    if (!resp.getLeaderHint().isBlank()) {
                        leaderHint = resp.getLeaderHint();
                        leader_cache.put(partition, leaderHint);
                    }

                    if (resp.getStatus() == RequestStatus.UNAVAILABLE) {
                        lastMessage = resp.getMessage().isBlank()
                                ? "DELETE failed for key " + key
                                        + ". Partition unavailable or commit did not reach quorum."
                                : "DELETE failed for key " + key + ". " + resp.getMessage();
                    } else if (resp.getStatus() == RequestStatus.NOT_LEADER) {
                        lastMessage = "DELETE failed for key " + key + ". Leader changed during routing.";
                    }
                } catch (Exception e) {
                    if (target.equals(leader_cache.get(partition))) {
                        leader_cache.remove(partition);
                    }
                    lastMessage = "DELETE failed for key " + key + ". RPC error while contacting " + target + ".";
                }
            }

            sleepQuietly(verbose ? VERBOSE_RETRY_SLEEP_MS : WORKLOAD_RETRY_SLEEP_MS);
        }

        return new RequestOutcome(false, lastMessage);
    }

    private void safeRefreshPartitionMap() {
        try {
            refreshPartitionMapFromSeed();
        } catch (Exception e) {
            // ignore and continue with best-known map
        }
    }

    private void sleepQuietly(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private List<String> buildAttemptOrder(int partition, String leaderHint) {
        List<String> replicas = partition_map.getOrDefault(partition, List.of());
        if (replicas.isEmpty()) {
            return List.of();
        }

        List<String> ordered = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        if (leaderHint != null && !leaderHint.isBlank() && replicas.contains(leaderHint)) {
            ordered.add(leaderHint);
            seen.add(leaderHint);
        }

        String cached = leader_cache.get(partition);
        if (cached != null && !cached.isBlank() && replicas.contains(cached) && !seen.contains(cached)) {
            ordered.add(cached);
            seen.add(cached);
        }

        List<String> shuffled = new ArrayList<>(replicas);
        Collections.shuffle(shuffled, random);
        for (String replica : shuffled) {
            if (!seen.contains(replica)) {
                ordered.add(replica);
                seen.add(replica);
            }
        }

        return ordered;
    }

    private String chooseNextTarget(List<String> currentOrder, Set<String> tried) {
        for (String candidate : currentOrder) {
            if (!tried.contains(candidate)) {
                return candidate;
            }
        }
        return null;
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
                        "\nAlive nodes=" + resp.getAlive() +
                        "\nHealthy partitions=" + resp.getHealthy() +
                        "\nWeak partitions=" + resp.getWeak() +
                        "\nDead partitions=" + resp.getDead());
    }

    private static class RequestOutcome {
        final boolean success;
        final String message;

        RequestOutcome(boolean success, String message) {
            this.success = success;
            this.message = message == null ? "" : message;
        }
    }
}