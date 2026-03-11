package com.evan.data;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.evan.core.Config;

public class DataConnection {
    private final Config config;
    private final String db_name;

    public static class MvccVersion {
        private final int version;
        private final long term;
        private final String op;
        private final String key;
        private final String value;
        private final boolean deleted;
        private final String requestId;

        public MvccVersion(int version, long term, String op, String key, String value, boolean deleted,
                String requestId) {
            this.version = version;
            this.term = term;
            this.op = op;
            this.key = key;
            this.value = value;
            this.deleted = deleted;
            this.requestId = requestId;
        }

        public int getVersion() {
            return version;
        }

        public long getTerm() {
            return term;
        }

        public String getOp() {
            return op;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public boolean isDeleted() {
            return deleted;
        }

        public String getRequestId() {
            return requestId;
        }
    }

    public DataConnection(String db_name) {
        this.config = new Config().load_parameters();
        this.db_name = db_name;
    }

    public Connection connectAndInitialize() {
        Connection conn = null;

        String kv_sql = "CREATE TABLE IF NOT EXISTS kv_store (" +
                "partition_id INTEGER NOT NULL, " +
                "key TEXT NOT NULL, " +
                "value TEXT, " +
                "PRIMARY KEY (partition_id, key));";

        String kv_history_sql = "CREATE TABLE IF NOT EXISTS kv_history (" +
                "partition_id INTEGER NOT NULL, " +
                "key TEXT NOT NULL, " +
                "version INTEGER NOT NULL, " +
                "term INTEGER NOT NULL, " +
                "op TEXT NOT NULL, " +
                "value TEXT, " +
                "is_deleted INTEGER NOT NULL DEFAULT 0, " +
                "request_id TEXT, " +
                "PRIMARY KEY (partition_id, key, version));";

        String metadata_sql = "CREATE TABLE IF NOT EXISTS raft_meta (" +
                "partition_id INTEGER PRIMARY KEY, " +
                "current_term INTEGER NOT NULL DEFAULT 0, " +
                "voted_for TEXT, " +
                "commit_index INTEGER NOT NULL DEFAULT 0, " +
                "last_applied INTEGER NOT NULL DEFAULT 0" +
                ");";

        String log_sql = "CREATE TABLE IF NOT EXISTS raft_log (" +
                "partition_id INTEGER NOT NULL, " +
                "log_index INTEGER NOT NULL, " +
                "term INTEGER NOT NULL, " +
                "op TEXT NOT NULL, " +
                "key TEXT NOT NULL, " +
                "value TEXT, " +
                "request_id TEXT, " +
                "PRIMARY KEY (partition_id, log_index)" +
                ");";

        try {
            String cur_dir = System.getProperty("user.dir");
            String data_dir = cur_dir + config.getDatabase().getPath();
            File data_folder = new File(data_dir);
            if (!data_folder.exists()) {
                data_folder.mkdirs();
            }

            String url = "jdbc:sqlite:" + data_dir + this.db_name + ".db";
            conn = DriverManager.getConnection(url);

            Statement stmt = conn.createStatement();
            stmt.execute("PRAGMA journal_mode=WAL;");
            stmt.execute("PRAGMA synchronous=NORMAL;");
            stmt.execute(kv_sql);
            stmt.execute(kv_history_sql);
            stmt.execute(metadata_sql);
            stmt.execute(log_sql);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    public boolean put(Connection conn, int partition_id, String key, String value) {
        String sql = "INSERT OR REPLACE INTO kv_store(partition_id, key, value) VALUES (?, ?, ?);";
        try {
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1, partition_id);
            pstmt.setString(2, key);
            pstmt.setString(3, value);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return false;
        }
        return true;
    }

    public String get(Connection conn, int partition_id, String key) {
        String sql = "SELECT value FROM kv_store WHERE partition_id = ? AND key = ?;";
        try {
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1, partition_id);
            pstmt.setString(2, key);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString("value");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }
        return null;
    }

    public boolean delete(Connection conn, int partition_id, String key) {
        String sql = "DELETE FROM kv_store WHERE partition_id = ? AND key = ?;";
        try {
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1, partition_id);
            pstmt.setString(2, key);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return false;
        }
        return true;
    }

    public boolean applyPutVersion(Connection conn, int partitionId, int version, long term, String key, String value,
            String requestId) {
        String insertHistorySql = "INSERT OR REPLACE INTO kv_history " +
                "(partition_id, key, version, term, op, value, is_deleted, request_id) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?);";

        try {
            PreparedStatement historyStmt = conn.prepareStatement(insertHistorySql);
            historyStmt.setInt(1, partitionId);
            historyStmt.setString(2, key);
            historyStmt.setInt(3, version);
            historyStmt.setLong(4, term);
            historyStmt.setString(5, "PUT");
            historyStmt.setString(6, value);
            historyStmt.setInt(7, 0);
            historyStmt.setString(8, requestId);
            historyStmt.executeUpdate();

            return put(conn, partitionId, key, value);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

    public boolean applyDeleteVersion(Connection conn, int partitionId, int version, long term, String key,
            String requestId) {
        String insertHistorySql = "INSERT OR REPLACE INTO kv_history " +
                "(partition_id, key, version, term, op, value, is_deleted, request_id) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?);";

        try {
            PreparedStatement historyStmt = conn.prepareStatement(insertHistorySql);
            historyStmt.setInt(1, partitionId);
            historyStmt.setString(2, key);
            historyStmt.setInt(3, version);
            historyStmt.setLong(4, term);
            historyStmt.setString(5, "DELETE");
            historyStmt.setString(6, null);
            historyStmt.setInt(7, 1);
            historyStmt.setString(8, requestId);
            historyStmt.executeUpdate();

            return delete(conn, partitionId, key);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

    public List<MvccVersion> getHistory(Connection conn, int partitionId, String key) {
        String sql = "SELECT version, term, op, key, value, is_deleted, request_id " +
                "FROM kv_history WHERE partition_id = ? AND key = ? " +
                "ORDER BY version DESC;";

        List<MvccVersion> versions = new ArrayList<>();

        try {
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1, partitionId);
            pstmt.setString(2, key);

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                versions.add(new MvccVersion(
                        rs.getInt("version"),
                        rs.getLong("term"),
                        rs.getString("op"),
                        rs.getString("key"),
                        rs.getString("value"),
                        rs.getInt("is_deleted") == 1,
                        rs.getString("request_id")));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return versions;
    }

    public MvccVersion getVersion(Connection conn, int partitionId, String key, int version) {
        String sql = "SELECT version, term, op, key, value, is_deleted, request_id " +
                "FROM kv_history WHERE partition_id = ? AND key = ? AND version = ?;";

        try {
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1, partitionId);
            pstmt.setString(2, key);
            pstmt.setInt(3, version);

            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return new MvccVersion(
                        rs.getInt("version"),
                        rs.getLong("term"),
                        rs.getString("op"),
                        rs.getString("key"),
                        rs.getString("value"),
                        rs.getInt("is_deleted") == 1,
                        rs.getString("request_id"));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return null;
    }
}