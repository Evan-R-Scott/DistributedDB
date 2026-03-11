package com.evan.core;

import java.io.InputStream;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Config {
    private Cluster cluster;
    private Seed seed;
    private Heartbeat heartbeat;
    private Router router;
    private Database database;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Config() {
    }

    public Config load_parameters() {
        try (InputStream inputStream = Config.class.getClassLoader().getResourceAsStream("config.json")) {
            if (inputStream == null) {
                throw new RuntimeException("config.json not found on classpath");
            }

            return objectMapper.readValue(inputStream, Config.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config.json", e);
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public Seed getSeed() {
        return seed;
    }

    public void setSeed(Seed seed) {
        this.seed = seed;
    }

    public Heartbeat getHeartbeat() {
        return heartbeat;
    }

    public void setHeartbeat(Heartbeat heartbeat) {
        this.heartbeat = heartbeat;
    }

    public Router getRouter() {
        return router;
    }

    public void setRouter(Router router) {
        this.router = router;
    }

    public Database getDatabase() {
        return database;
    }

    public void setDatabase(Database database) {
        this.database = database;
    }

    public static class Cluster {
        private int num_partitions;
        private int replication_factor;

        public Cluster() {
        }

        public int getNumPartitions() {
            return num_partitions;
        }

        public void setNumPartitions(int num_partitions) {
            this.num_partitions = num_partitions;
        }

        public int getReplicationFactor() {
            return replication_factor;
        }

        public void setReplicationFactor(int replication_factor) {
            this.replication_factor = replication_factor;
        }
    }

    public static class Seed {
        private String address;
        private int membership_stable_ms;
        private int cleanup_interval_seconds;

        public Seed() {
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public int getMembershipStableMs() {
            return membership_stable_ms;
        }

        public void setMembershipStableMs(int membership_stable_ms) {
            this.membership_stable_ms = membership_stable_ms;
        }

        public int getCleanupIntervalSeconds() {
            return cleanup_interval_seconds;
        }

        public void setCleanupIntervalSeconds(int cleanup_interval_seconds) {
            this.cleanup_interval_seconds = cleanup_interval_seconds;
        }
    }

    public static class Heartbeat {
        private int ttl_ms;

        public Heartbeat() {
        }

        public int getTtlMs() {
            return ttl_ms;
        }

        public void setTtlMs(int ttl_ms) {
            this.ttl_ms = ttl_ms;
        }
    }

    public static class Router {
        private int rpc_timeout_ms;
        private int max_retries;

        public Router() {
        }

        public int getRpcTimeoutMs() {
            return rpc_timeout_ms;
        }

        public void setRpcTimeoutMs(int rpc_timeout_ms) {
            this.rpc_timeout_ms = rpc_timeout_ms;
        }

        public int getMaxRetries() {
            return max_retries;
        }

        public void setMaxRetries(int max_retries) {
            this.max_retries = max_retries;
        }
    }

    public static class Database {
        private String path;

        public Database() {
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }
    }
}