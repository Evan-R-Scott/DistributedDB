package com.evan.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionReplica {
    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    private final int partitionId;

    private volatile Role role = Role.FOLLOWER;
    private volatile long currentTerm = 0;
    private volatile String votedFor = null;
    private volatile String knownLeaderId = null;

    private final List<LogEntry> log = new ArrayList<>();

    private volatile int commitIndex = 0;
    private volatile int lastApplied = 0;

    private volatile long electionDeadlineMs = 0L;
    private volatile long lastHeartbeatMs = 0L;

    // Fixed Raft membership for this partition (never shrinks after first full
    // view).
    private volatile List<String> replicaSet = List.of();

    // Current live replicas from seed active map.
    private volatile List<String> activeReplicaSet = List.of();

    // Rejoin stabilization
    private volatile boolean passiveRejoin = false;
    private volatile boolean heardFromLeaderSinceRejoin = false;
    private volatile long passiveRejoinUntilMs = 0L;

    private final Map<String, Integer> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Integer> matchIndex = new ConcurrentHashMap<>();

    public PartitionReplica(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public synchronized Role getRole() {
        return role;
    }

    public synchronized void setRole(Role role) {
        this.role = role;
    }

    public synchronized long getCurrentTerm() {
        return currentTerm;
    }

    public synchronized void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
    }

    public synchronized String getVotedFor() {
        return votedFor;
    }

    public synchronized void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
    }

    public synchronized String getKnownLeaderId() {
        return knownLeaderId;
    }

    public synchronized void setKnownLeaderId(String knownLeaderId) {
        this.knownLeaderId = knownLeaderId;
    }

    public synchronized int getCommitIndex() {
        return commitIndex;
    }

    public synchronized void setCommitIndex(int commitIndex) {
        this.commitIndex = commitIndex;
    }

    public synchronized int getLastApplied() {
        return lastApplied;
    }

    public synchronized void setLastApplied(int lastApplied) {
        this.lastApplied = lastApplied;
    }

    public synchronized long getElectionDeadlineMs() {
        return electionDeadlineMs;
    }

    public synchronized void setElectionDeadlineMs(long electionDeadlineMs) {
        this.electionDeadlineMs = electionDeadlineMs;
    }

    public synchronized long getLastHeartbeatMs() {
        return lastHeartbeatMs;
    }

    public synchronized void setLastHeartbeatMs(long lastHeartbeatMs) {
        this.lastHeartbeatMs = lastHeartbeatMs;
    }

    public synchronized List<String> getReplicaSet() {
        return replicaSet;
    }

    public synchronized void setReplicaSet(List<String> replicaSet) {
        this.replicaSet = List.copyOf(replicaSet);
    }

    public synchronized List<String> getActiveReplicaSet() {
        return activeReplicaSet;
    }

    public synchronized void setActiveReplicaSet(List<String> activeReplicaSet) {
        this.activeReplicaSet = List.copyOf(activeReplicaSet);
    }

    public synchronized boolean isPassiveRejoin() {
        return passiveRejoin;
    }

    public synchronized void setPassiveRejoin(boolean passiveRejoin) {
        this.passiveRejoin = passiveRejoin;
    }

    public synchronized boolean hasHeardFromLeaderSinceRejoin() {
        return heardFromLeaderSinceRejoin;
    }

    public synchronized long getPassiveRejoinUntilMs() {
        return passiveRejoinUntilMs;
    }

    public synchronized void setPassiveRejoinUntilMs(long passiveRejoinUntilMs) {
        this.passiveRejoinUntilMs = passiveRejoinUntilMs;
    }

    public synchronized void markHeardFromLeaderSinceRejoin() {
        this.heardFromLeaderSinceRejoin = true;
        this.passiveRejoin = false;
        this.passiveRejoinUntilMs = 0L;
    }

    public synchronized void resetRejoinState(long passiveUntilMs) {
        this.heardFromLeaderSinceRejoin = false;
        this.passiveRejoin = true;
        this.passiveRejoinUntilMs = passiveUntilMs;
    }

    public synchronized void clearPassiveRejoin() {
        this.passiveRejoin = false;
        this.passiveRejoinUntilMs = 0L;
    }

    public Map<String, Integer> getNextIndex() {
        return nextIndex;
    }

    public Map<String, Integer> getMatchIndex() {
        return matchIndex;
    }

    public synchronized int lastLogIndex() {
        return log.size();
    }

    public synchronized long lastLogTerm() {
        if (log.isEmpty()) {
            return 0L;
        }
        return log.get(log.size() - 1).getTerm();
    }

    public synchronized long termAt(int oneBasedIndex) {
        if (oneBasedIndex <= 0 || oneBasedIndex > log.size()) {
            return 0L;
        }
        return log.get(oneBasedIndex - 1).getTerm();
    }

    public synchronized LogEntry getEntry(int oneBasedIndex) {
        if (oneBasedIndex <= 0 || oneBasedIndex > log.size()) {
            return null;
        }
        return log.get(oneBasedIndex - 1);
    }

    public synchronized List<LogEntry> getEntriesFrom(int oneBasedStartIndex) {
        if (oneBasedStartIndex <= 0) {
            oneBasedStartIndex = 1;
        }
        if (oneBasedStartIndex > log.size()) {
            return List.of();
        }
        return new ArrayList<>(log.subList(oneBasedStartIndex - 1, log.size()));
    }

    public synchronized void appendEntry(LogEntry entry) {
        log.add(entry);
    }

    public synchronized void deleteFrom(int oneBasedStartIndex) {
        if (oneBasedStartIndex <= 0 || oneBasedStartIndex > log.size()) {
            return;
        }
        log.subList(oneBasedStartIndex - 1, log.size()).clear();

        if (commitIndex > log.size()) {
            commitIndex = log.size();
        }
        if (lastApplied > log.size()) {
            lastApplied = log.size();
        }
    }

    public synchronized boolean candidateLogAtLeastUpToDate(int candidateLastIndex, long candidateLastTerm) {
        long myLastTerm = lastLogTerm();
        int myLastIndex = lastLogIndex();

        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= myLastIndex;
    }

    public synchronized void replaceLog(List<LogEntry> entries) {
        log.clear();
        log.addAll(entries);
    }

    public synchronized void hydrateFromDisk(
            long currentTerm,
            String votedFor,
            int commitIndex,
            int lastApplied,
            List<LogEntry> entries) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
        this.role = Role.FOLLOWER;
        this.knownLeaderId = null;
        this.log.clear();
        this.log.addAll(entries);
        this.nextIndex.clear();
        this.matchIndex.clear();
        this.passiveRejoin = false;
        this.heardFromLeaderSinceRejoin = false;
        this.passiveRejoinUntilMs = 0L;
    }
}