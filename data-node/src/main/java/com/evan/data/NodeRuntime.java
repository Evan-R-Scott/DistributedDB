package com.evan.data;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.evan.core.ChannelManager;
import com.evan.core.Config;
import com.evan.core.Hash;
import com.evan.proto.AppendEntriesRequest;
import com.evan.proto.AppendEntriesResponse;
import com.evan.proto.LogEntryMessage;
import com.evan.proto.NodeGrpc;
import com.evan.proto.VoteRequest;
import com.evan.proto.VoteResponse;

public class NodeRuntime {
    private static final long PASSIVE_REJOIN_GRACE_MS = 2500L;

    public static volatile boolean TRACE_ENABLED = true;

    private final ExecutorService replicationExecutor = Executors.newFixedThreadPool(32);
    private final Set<String> inFlightReplications = ConcurrentHashMap.newKeySet();

    private final Config config;
    private final ChannelManager channelManager;
    private final Connection connection;
    private final DataConnection dataConnection;
    private final RaftStore raftStore;
    private final Random random = new Random();

    private volatile String nodeId;
    private volatile String ip;
    private volatile int port;

    private final ConcurrentHashMap<Integer, List<String>> partitionMap;
    private final ConcurrentHashMap<Integer, PartitionReplica> replicas;

    public NodeRuntime(
            Config config,
            ChannelManager channelManager,
            Connection connection,
            DataConnection dataConnection) {
        this.config = config;
        this.channelManager = channelManager;
        this.connection = connection;
        this.dataConnection = dataConnection;
        this.raftStore = new RaftStore(connection);
        this.partitionMap = new ConcurrentHashMap<>();
        this.replicas = new ConcurrentHashMap<>();
    }

    public Config getConfig() {
        return config;
    }

    public ChannelManager getChannelManager() {
        return channelManager;
    }

    public Connection getConnection() {
        return connection;
    }

    public DataConnection getDataConnection() {
        return dataConnection;
    }

    public RaftStore getRaftStore() {
        return raftStore;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeIdentity(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.nodeId = ip + ":" + port;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public Map<Integer, List<String>> getPartitionMap() {
        return partitionMap;
    }

    public Map<Integer, PartitionReplica> getReplicas() {
        return replicas;
    }

    public int getPartitionForKey(String key) {
        return Hash.getPartition(key, config.getCluster().getNumPartitions());
    }

    public int fixedQuorumSize() {
        return (config.getCluster().getReplicationFactor() / 2) + 1;
    }

    public int activeReplicaCount(int partitionId) {
        PartitionReplica replica = replicas.get(partitionId);
        if (replica == null) {
            return 0;
        }
        return replica.getActiveReplicaSet().size();
    }

    public boolean canServeWrites(int partitionId) {
        return activeReplicaCount(partitionId) >= fixedQuorumSize();
    }

    public boolean canServeReads(int partitionId) {
        return activeReplicaCount(partitionId) >= fixedQuorumSize();
    }

    public synchronized void updatePartitionMap(Map<Integer, List<String>> newMap) {
        partitionMap.clear();
        partitionMap.putAll(newMap);

        List<LeaderCatchupTarget> immediateCatchups = new ArrayList<>();

        for (int partitionId = 0; partitionId < config.getCluster().getNumPartitions(); partitionId++) {
            List<String> activeSet = List.copyOf(newMap.getOrDefault(partitionId, List.of()));
            PartitionReplica replica = replicas.get(partitionId);

            boolean iAmActive = activeSet.contains(nodeId);

            if (replica == null && iAmActive) {
                replica = createReplicaFromDisk(partitionId);
                replicas.put(partitionId, replica);
            }

            if (replica == null) {
                continue;
            }

            synchronized (replica) {
                List<String> oldFrozen = replica.getReplicaSet();
                List<String> oldActive = replica.getActiveReplicaSet();

                replica.setActiveReplicaSet(activeSet);

                Set<String> frozenUnion = new HashSet<>(oldFrozen);
                frozenUnion.addAll(activeSet);

                List<String> newFrozen = new ArrayList<>(frozenUnion);
                newFrozen.sort(String::compareTo);
                replica.setReplicaSet(newFrozen);

                if (replica.isPassiveRejoin() && activeSet.size() >= fixedQuorumSize()
                        && System.currentTimeMillis() >= replica.getPassiveRejoinUntilMs()) {
                    replica.clearPassiveRejoin();
                    resetElectionDeadline(replica);
                }

                if (replica.getRole() == PartitionReplica.Role.LEADER) {
                    Set<String> oldActivePeers = new HashSet<>(oldActive);
                    for (String peer : activeSet) {
                        if (peer.equals(nodeId)) {
                            continue;
                        }

                        replica.getNextIndex().putIfAbsent(peer, replica.lastLogIndex() + 1);
                        replica.getMatchIndex().putIfAbsent(peer, 0);

                        if (!oldActivePeers.contains(peer)) {
                            immediateCatchups.add(new LeaderCatchupTarget(partitionId, peer));
                        }
                    }
                }
            }
        }

        List<Integer> toRemove = new ArrayList<>();
        for (Map.Entry<Integer, PartitionReplica> entry : replicas.entrySet()) {
            int partitionId = entry.getKey();
            PartitionReplica replica = entry.getValue();
            if (!replica.getActiveReplicaSet().contains(nodeId)) {
                toRemove.add(partitionId);
            }
        }

        for (Integer partitionId : toRemove) {
            replicas.remove(partitionId);
        }

        for (LeaderCatchupTarget target : immediateCatchups) {
            submitReplication(target.partitionId(), target.peerNodeId());
        }
    }

    private PartitionReplica createReplicaFromDisk(int partitionId) {
        PartitionReplica replica = new PartitionReplica(partitionId);

        RaftStore.PersistedMeta meta = raftStore.loadMeta(partitionId);
        List<LogEntry> persistedLog = raftStore.loadLog(partitionId);

        int safeCommitIndex = Math.min(meta.getCommitIndex(), persistedLog.size());
        int safeLastApplied = Math.min(meta.getLastApplied(), safeCommitIndex);

        replica.hydrateFromDisk(
                meta.getCurrentTerm(),
                meta.getVotedFor(),
                safeCommitIndex,
                safeLastApplied,
                persistedLog);

        long now = System.currentTimeMillis();
        replica.setLastHeartbeatMs(now);
        resetElectionDeadline(replica);

        return replica;
    }

    public void enterPassiveRejoinMode() {
        long now = System.currentTimeMillis();
        long passiveUntil = now + PASSIVE_REJOIN_GRACE_MS;

        for (PartitionReplica replica : replicas.values()) {
            synchronized (replica) {
                replica.setRole(PartitionReplica.Role.FOLLOWER);
                replica.setKnownLeaderId(null);
                replica.setLastHeartbeatMs(now);
                replica.resetRejoinState(passiveUntil);
                replica.setElectionDeadlineMs(passiveUntil);
            }
        }

        trace("node is rejoining");
    }

    public void resetElectionDeadline(PartitionReplica replica) {
        long timeoutMs = 5000L + random.nextInt(5000);
        replica.setElectionDeadlineMs(System.currentTimeMillis() + timeoutMs);
    }

    public String getLeaderHint(int partitionId) {
        PartitionReplica replica = replicas.get(partitionId);
        if (replica == null) {
            return "";
        }
        String leader = replica.getKnownLeaderId();
        return leader == null ? "" : leader;
    }

    public boolean isLeader(int partitionId) {
        PartitionReplica replica = replicas.get(partitionId);
        return replica != null && replica.getRole() == PartitionReplica.Role.LEADER;
    }

    public void replayCommittedEntriesOnStartup() {
        for (Integer partitionId : replicas.keySet()) {
            replayCommittedEntriesForPartition(partitionId);
        }
    }

    public void replayCommittedEntriesForPartition(int partitionId) {
        PartitionReplica replica = replicas.get(partitionId);
        if (replica == null) {
            return;
        }

        synchronized (replica) {
            while (replica.getLastApplied() < replica.getCommitIndex()) {
                int nextToApply = replica.getLastApplied() + 1;
                LogEntry toApply = replica.getEntry(nextToApply);
                if (toApply == null) {
                    return;
                }

                synchronized (connection) {
                    if (toApply.getOp() == LogEntry.OperationType.PUT) {
                        dataConnection.applyPutVersion(
                                connection,
                                partitionId,
                                toApply.getIndex(),
                                toApply.getTerm(),
                                toApply.getKey(),
                                toApply.getValue(),
                                toApply.getRequestId());
                    } else if (toApply.getOp() == LogEntry.OperationType.DELETE) {
                        dataConnection.applyDeleteVersion(
                                connection,
                                partitionId,
                                toApply.getIndex(),
                                toApply.getTerm(),
                                toApply.getKey(),
                                toApply.getRequestId());
                    }
                }

                replica.setLastApplied(nextToApply);
                persistMeta(replica);
            }
        }
    }

    public void runElectionTick() {
        long now = System.currentTimeMillis();

        for (PartitionReplica replica : replicas.values()) {
            if (replica.getRole() == PartitionReplica.Role.LEADER) {
                continue;
            }

            if (replica.isPassiveRejoin()) {
                if (now >= replica.getPassiveRejoinUntilMs()
                        && replica.getActiveReplicaSet().size() >= fixedQuorumSize()) {
                    replica.clearPassiveRejoin();
                    resetElectionDeadline(replica);
                } else {
                    continue;
                }
            }

            if (replica.getActiveReplicaSet().size() < fixedQuorumSize()) {
                continue;
            }

            if (now >= replica.getElectionDeadlineMs()) {
                startElection(replica.getPartitionId());
            }
        }
    }

    public void startElection(int partitionId) {
        PartitionReplica replica = replicas.get(partitionId);
        if (replica == null) {
            return;
        }

        List<String> frozenPeers;
        List<String> activePeers;
        long term;
        int lastLogIndex;
        long lastLogTerm;

        synchronized (replica) {
            if (replica.isPassiveRejoin()) {
                return;
            }

            if (replica.getActiveReplicaSet().size() < fixedQuorumSize()) {
                return;
            }

            replica.setRole(PartitionReplica.Role.CANDIDATE);
            replica.setCurrentTerm(replica.getCurrentTerm() + 1);
            replica.setVotedFor(nodeId);
            replica.setKnownLeaderId(null);
            resetElectionDeadline(replica);

            persistMeta(replica);

            frozenPeers = new ArrayList<>(replica.getReplicaSet());
            activePeers = new ArrayList<>(replica.getActiveReplicaSet());
            term = replica.getCurrentTerm();
            lastLogIndex = replica.lastLogIndex();
            lastLogTerm = replica.lastLogTerm();
        }

        int votes = 1;
        int quorum = fixedQuorumSize();

        for (String peer : frozenPeers) {
            if (peer.equals(nodeId)) {
                continue;
            }

            if (!activePeers.contains(peer)) {
                continue;
            }

            try {
                NodeGrpc.NodeBlockingStub stub = blockingStubForNodeId(peer);

                VoteResponse response = stub.withDeadlineAfter(400, TimeUnit.MILLISECONDS).requestVote(
                        VoteRequest.newBuilder()
                                .setPartitionId(partitionId)
                                .setTerm(term)
                                .setCandidateId(nodeId)
                                .setLastLogIndex(lastLogIndex)
                                .setLastLogTerm(lastLogTerm)
                                .build());

                synchronized (replica) {
                    if (response.getTerm() > replica.getCurrentTerm()) {
                        replica.setCurrentTerm(response.getTerm());
                        replica.setRole(PartitionReplica.Role.FOLLOWER);
                        replica.setVotedFor(null);
                        replica.setKnownLeaderId(null);
                        persistMeta(replica);
                        resetElectionDeadline(replica);
                        return;
                    }
                }

                if (response.getVoteGranted()) {
                    votes++;
                }
            } catch (Exception e) {
                // peer unreachable this round
            }
        }

        synchronized (replica) {
            if (replica.getRole() != PartitionReplica.Role.CANDIDATE) {
                return;
            }
            if (replica.getCurrentTerm() != term) {
                return;
            }

            if (votes >= quorum) {
                replica.setRole(PartitionReplica.Role.LEADER);
                replica.setKnownLeaderId(nodeId);
                replica.setPassiveRejoin(false);

                for (String peer : replica.getReplicaSet()) {
                    if (!peer.equals(nodeId)) {
                        replica.getNextIndex().put(peer, replica.lastLogIndex() + 1);
                        replica.getMatchIndex().putIfAbsent(peer, 0);
                    }
                }

                trace("leader elected for partition " + partitionId);
            } else {
                replica.setRole(PartitionReplica.Role.FOLLOWER);
                replica.setKnownLeaderId(null);
                resetElectionDeadline(replica);
            }

            persistMeta(replica);
        }
    }

    public void runHeartbeatTick() {
        for (PartitionReplica replica : replicas.values()) {
            if (replica.getRole() != PartitionReplica.Role.LEADER) {
                continue;
            }

            if (replica.getActiveReplicaSet().size() < fixedQuorumSize()) {
                synchronized (replica) {
                    replica.setRole(PartitionReplica.Role.FOLLOWER);
                    replica.setKnownLeaderId(null);
                    resetElectionDeadline(replica);
                    persistMeta(replica);
                }
                continue;
            }

            List<String> activePeers = replica.getActiveReplicaSet();
            for (String peer : activePeers) {
                if (peer.equals(nodeId)) {
                    continue;
                }
                submitReplication(replica.getPartitionId(), peer);
            }
        }
    }

    public boolean appendClientWrite(int partitionId, LogEntry.OperationType op, String key, String value) {
        PartitionReplica replica = replicas.get(partitionId);
        if (replica == null) {
            return false;
        }

        if (!canServeWrites(partitionId)) {
            return false;
        }

        int entryIndex;
        long termAtAppend;

        synchronized (replica) {
            if (replica.getRole() != PartitionReplica.Role.LEADER) {
                return false;
            }

            if (replica.getActiveReplicaSet().size() < fixedQuorumSize()) {
                return false;
            }

            entryIndex = replica.lastLogIndex() + 1;
            termAtAppend = replica.getCurrentTerm();

            LogEntry entry = new LogEntry(
                    entryIndex,
                    termAtAppend,
                    op,
                    key,
                    value,
                    UUID.randomUUID().toString());

            raftStore.appendLogEntry(partitionId, entry);
            replica.appendEntry(entry);
            persistMeta(replica);

            if (op == LogEntry.OperationType.PUT) {
                trace("leader put key: " + key + ", value: " + value);
            } else if (op == LogEntry.OperationType.DELETE) {
                trace("leader delete key: " + key);
            }
        }

        List<String> activePeers = replica.getActiveReplicaSet();
        for (String peer : activePeers) {
            if (peer.equals(nodeId)) {
                continue;
            }
            sendAppendEntries(partitionId, peer);
        }

        advanceCommitIndex(partitionId);

        for (String peer : activePeers) {
            if (peer.equals(nodeId)) {
                continue;
            }
            submitReplication(partitionId, peer);
        }

        long deadline = System.currentTimeMillis() + 5000;
        synchronized (replica) {
            while (replica.getCommitIndex() < entryIndex) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0)
                    return false;
                try {
                    replica.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
                if (replica.getRole() != PartitionReplica.Role.LEADER)
                    return false;
            }
            return true;
        }
    }

    public void applyCommittedEntries() {
        for (Integer partitionId : replicas.keySet()) {
            applyCommittedEntriesForPartition(partitionId);
        }
    }

    public void applyCommittedEntriesForPartition(int partitionId) {
        replayCommittedEntriesForPartition(partitionId);
    }

    public String getValue(String key) {
        int partitionId = getPartitionForKey(key);
        synchronized (connection) {
            return dataConnection.get(connection, partitionId, key);
        }
    }

    public List<DataConnection.MvccVersion> getHistory(String key) {
        int partitionId = getPartitionForKey(key);
        synchronized (connection) {
            return dataConnection.getHistory(connection, partitionId, key);
        }
    }

    public DataConnection.MvccVersion getVersion(String key, int version) {
        int partitionId = getPartitionForKey(key);
        synchronized (connection) {
            return dataConnection.getVersion(connection, partitionId, key, version);
        }
    }

    public boolean sendAppendEntries(int partitionId, String followerNodeId) {
        PartitionReplica replica = replicas.get(partitionId);
        if (replica == null) {
            return false;
        }

        long term;
        int prevLogIndex;
        long prevLogTerm;
        int leaderCommit;
        List<LogEntry> entriesToSend;

        synchronized (replica) {
            if (replica.getRole() != PartitionReplica.Role.LEADER) {
                return false;
            }

            if (!replica.getActiveReplicaSet().contains(followerNodeId)) {
                return false;
            }

            int nextIndex = replica.getNextIndex().getOrDefault(followerNodeId, replica.lastLogIndex() + 1);
            prevLogIndex = nextIndex - 1;
            prevLogTerm = replica.termAt(prevLogIndex);
            entriesToSend = replica.getEntriesFrom(nextIndex);
            term = replica.getCurrentTerm();
            leaderCommit = replica.getCommitIndex();
        }

        AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder()
                .setPartitionId(partitionId)
                .setTerm(term)
                .setLeaderId(nodeId)
                .setPrevLogIndex(prevLogIndex)
                .setPrevLogTerm(prevLogTerm)
                .setLeaderCommit(leaderCommit);

        for (LogEntry entry : entriesToSend) {
            builder.addEntries(LogEntryMessage.newBuilder()
                    .setIndex(entry.getIndex())
                    .setTerm(entry.getTerm())
                    .setOp(entry.getOp().name())
                    .setKey(entry.getKey() == null ? "" : entry.getKey())
                    .setValue(entry.getValue() == null ? "" : entry.getValue())
                    .setRequestId(entry.getRequestId() == null ? "" : entry.getRequestId())
                    .build());
        }

        try {
            NodeGrpc.NodeBlockingStub stub = blockingStubForNodeId(followerNodeId);
            AppendEntriesResponse response = stub.withDeadlineAfter(750, TimeUnit.MILLISECONDS)
                    .appendEntries(builder.build());

            synchronized (replica) {
                if (response.getTerm() > replica.getCurrentTerm()) {
                    replica.setCurrentTerm(response.getTerm());
                    replica.setRole(PartitionReplica.Role.FOLLOWER);
                    replica.setVotedFor(null);
                    replica.setKnownLeaderId(null);
                    persistMeta(replica);
                    resetElectionDeadline(replica);
                    return false;
                }

                if (response.getSuccess()) {
                    int replicatedThrough = prevLogIndex + entriesToSend.size();
                    replica.getMatchIndex().put(followerNodeId, replicatedThrough);
                    replica.getNextIndex().put(followerNodeId, replicatedThrough + 1);
                    return true;
                } else {
                    int fallback = Math.max(1, response.getLastLogIndex() + 1);
                    replica.getNextIndex().put(followerNodeId, fallback);
                    return false;
                }
            }
        } catch (Exception e) {
            return false;
        }
    }

    public void advanceCommitIndex(int partitionId) {
        PartitionReplica replica = replicas.get(partitionId);
        if (replica == null) {
            return;
        }

        synchronized (replica) {
            if (replica.getRole() != PartitionReplica.Role.LEADER) {
                return;
            }

            if (replica.getActiveReplicaSet().size() < fixedQuorumSize()) {
                return;
            }

            int lastLogIndex = replica.lastLogIndex();
            int quorum = fixedQuorumSize();

            for (int idx = lastLogIndex; idx > replica.getCommitIndex(); idx--) {
                LogEntry entry = replica.getEntry(idx);
                if (entry == null) {
                    continue;
                }

                if (entry.getTerm() != replica.getCurrentTerm()) {
                    continue;
                }

                int replicatedCount = 1;
                for (String peer : replica.getReplicaSet()) {
                    if (peer.equals(nodeId)) {
                        continue;
                    }

                    if (!replica.getActiveReplicaSet().contains(peer)) {
                        continue;
                    }

                    int match = replica.getMatchIndex().getOrDefault(peer, 0);
                    if (match >= idx) {
                        replicatedCount++;
                    }
                }

                if (replicatedCount >= quorum) {
                    replica.setCommitIndex(idx);
                    persistMeta(replica);
                    replica.notifyAll();
                    break;
                }
            }
        }
    }

    private void persistMeta(PartitionReplica replica) {
        raftStore.saveMeta(
                replica.getPartitionId(),
                replica.getCurrentTerm(),
                replica.getVotedFor(),
                replica.getCommitIndex(),
                replica.getLastApplied());
    }

    private void submitReplication(int partitionId, String peer) {
        String key = partitionId + ":" + peer;
        if (!inFlightReplications.add(key)) {
            return;
        }
        replicationExecutor.submit(() -> {
            try {
                sendAppendEntries(partitionId, peer);
                advanceCommitIndex(partitionId);
            } finally {
                inFlightReplications.remove(key);
            }
        });
    }

    private NodeGrpc.NodeBlockingStub blockingStubForNodeId(String nodeId) {
        String[] parts = nodeId.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        return channelManager.getNodeBlocking(host, port);
    }

    private void trace(String message) {
        if (!TRACE_ENABLED) {
            return;
        }
        System.out.println(message);
    }

    private record LeaderCatchupTarget(int partitionId, String peerNodeId) {
    }
}