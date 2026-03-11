package com.evan.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class RaftStore {
    private final Connection conn;

    public static class PersistedMeta {
        private final long currentTerm;
        private final String votedFor;
        private final int commitIndex;
        private final int lastApplied;

        public PersistedMeta(long currentTerm, String votedFor, int commitIndex, int lastApplied) {
            this.currentTerm = currentTerm;
            this.votedFor = votedFor;
            this.commitIndex = commitIndex;
            this.lastApplied = lastApplied;
        }

        public long getCurrentTerm() {
            return currentTerm;
        }

        public String getVotedFor() {
            return votedFor;
        }

        public int getCommitIndex() {
            return commitIndex;
        }

        public int getLastApplied() {
            return lastApplied;
        }
    }

    public RaftStore(Connection conn) {
        this.conn = conn;
    }

    public synchronized PersistedMeta loadMeta(int partitionId) {
        String sql = "SELECT current_term, voted_for, commit_index, last_applied " +
                "FROM raft_meta WHERE partition_id = ?;";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, partitionId);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                return new PersistedMeta(
                        rs.getLong("current_term"),
                        rs.getString("voted_for"),
                        rs.getInt("commit_index"),
                        rs.getInt("last_applied"));
            }
        } catch (SQLException e) {
            System.out.println("loadMeta failed: " + e.getMessage());
        }

        return new PersistedMeta(0L, null, 0, 0);
    }

    public synchronized void saveMeta(int partitionId, long currentTerm, String votedFor, int commitIndex,
            int lastApplied) {
        String sql = "INSERT INTO raft_meta(partition_id, current_term, voted_for, commit_index, last_applied) " +
                "VALUES (?, ?, ?, ?, ?) " +
                "ON CONFLICT(partition_id) DO UPDATE SET " +
                "current_term = excluded.current_term, " +
                "voted_for = excluded.voted_for, " +
                "commit_index = excluded.commit_index, " +
                "last_applied = excluded.last_applied;";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, partitionId);
            pstmt.setLong(2, currentTerm);
            pstmt.setString(3, votedFor);
            pstmt.setInt(4, commitIndex);
            pstmt.setInt(5, lastApplied);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("saveMeta failed: " + e.getMessage());
        }
    }

    public synchronized void saveTermAndVote(int partitionId, long currentTerm, String votedFor) {
        PersistedMeta meta = loadMeta(partitionId);
        saveMeta(partitionId, currentTerm, votedFor, meta.getCommitIndex(), meta.getLastApplied());
    }

    public synchronized void saveCommitAndApply(int partitionId, int commitIndex, int lastApplied) {
        PersistedMeta meta = loadMeta(partitionId);
        saveMeta(partitionId, meta.getCurrentTerm(), meta.getVotedFor(), commitIndex, lastApplied);
    }

    public synchronized List<LogEntry> loadLog(int partitionId) {
        String sql = "SELECT log_index, term, op, key, value, request_id " +
                "FROM raft_log WHERE partition_id = ? ORDER BY log_index ASC;";

        List<LogEntry> entries = new ArrayList<>();

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, partitionId);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                LogEntry.OperationType op = LogEntry.OperationType.valueOf(rs.getString("op"));

                entries.add(new LogEntry(
                        rs.getInt("log_index"),
                        rs.getLong("term"),
                        op,
                        rs.getString("key"),
                        rs.getString("value"),
                        rs.getString("request_id")));
            }
        } catch (SQLException e) {
            System.out.println("loadLog failed: " + e.getMessage());
        }

        return entries;
    }

    public synchronized void appendLogEntry(int partitionId, LogEntry entry) {
        String sql = "INSERT OR REPLACE INTO raft_log(partition_id, log_index, term, op, key, value, request_id) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?);";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, partitionId);
            pstmt.setInt(2, entry.getIndex());
            pstmt.setLong(3, entry.getTerm());
            pstmt.setString(4, entry.getOp().name());
            pstmt.setString(5, entry.getKey());
            pstmt.setString(6, entry.getValue());
            pstmt.setString(7, entry.getRequestId());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("appendLogEntry failed: " + e.getMessage());
        }
    }

    public synchronized void deleteLogFrom(int partitionId, int startIndexInclusive) {
        String sql = "DELETE FROM raft_log WHERE partition_id = ? AND log_index >= ?;";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, partitionId);
            pstmt.setInt(2, startIndexInclusive);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("deleteLogFrom failed: " + e.getMessage());
        }
    }
}