package com.evan.core;

import java.io.File;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;;

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
        Config config = new Config();
        try {
            File parameter_json_file = new File("config.json");
            config = objectMapper.readValue(parameter_json_file, Config.class);
            return config;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return config;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Seed getSeed() {
        return seed;
    }

    public Heartbeat getHeartbeat() {
        return heartbeat;
    }

    public Router getRouter() {
        return router;
    }

    public Database getDatabase() {
        return database;
    }

    // read in the hyperparameter constants from config.json for all entities to use
    // from this shared module
    public class Cluster {
        private int num_partitions;
        private int replication_factor;

        public int getNumPartitions() {
            return num_partitions;
        }

        public int getReplicationFactor() {
            return replication_factor;
        }
    }

    public class Seed {
        private List<String> nodes;
        private int membership_stable_ms;
        private int cleanup_interval_seconds;

        public List<String> getNodes() {
            return nodes;
        }

        public int getMembershipStableMs() {
            return membership_stable_ms;
        }

        public int getCleanupIntervalSeconds() {
            return cleanup_interval_seconds;
        }
    }

    public class Heartbeat {
        private int interval_ms;
        private int ttl_ms;

        public int getIntervalMs() {
            return interval_ms;
        }

        public int getTtlMs() {
            return ttl_ms;
        }
    }

    public class Router {
        private int rpc_timeout_ms;
        private int max_retries;

        public int getRpcTimeoutMs() {
            return rpc_timeout_ms;
        }

        public int getMaxRetries() {
            return max_retries;
        }
    }

    public class Database {
        private String path;
        private String name;

        public String getPath() {
            return path;
        }

        public String getName() {
            return name;
        }
    }
}
