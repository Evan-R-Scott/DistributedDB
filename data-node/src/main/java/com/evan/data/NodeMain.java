package com.evan.data;

import java.io.IOException;
import java.net.InetAddress;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.evan.core.ChannelManager;
import com.evan.core.Config;
import com.evan.core.Seed;
import com.evan.proto.BootstrapSeedGrpc;
import com.evan.proto.PartitionMapResponse;
import com.evan.proto.PartitionSet;
import com.evan.proto.RegisterRequest;
import com.evan.proto.RegisterResponse;
import com.evan.proto.RegisterStatus;
import com.google.protobuf.Empty;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class NodeMain {
    private final Config config;
    private final NodeService nodeService;
    private final NodeRuntime runtime;
    private final ChannelManager channel_manager;

    private int port;
    private String ip;
    private Seed seed;

    private static final ScheduledExecutorService seedEx = Executors.newScheduledThreadPool(1);
    private static final ScheduledExecutorService raftEx = Executors.newScheduledThreadPool(16);
    private static final ScheduledExecutorService applyEx = Executors.newScheduledThreadPool(1);
    private BootstrapSeedGrpc.BootstrapSeedBlockingStub bootstrap_seed_stub;

    public NodeMain(int port) {
        this.config = new Config().load_parameters();
        this.port = port;

        try {
            this.ip = InetAddress.getLocalHost().getHostAddress();
        } catch (IOException e) {
            System.err.println("Error getting local host address: " + e.getMessage());
            System.exit(1);
        }

        // Keep localhost for local testing consistency.
        this.ip = "127.0.0.1";

        String db_name = "node_" + this.port;
        DataConnection dc = new DataConnection(db_name);
        Connection conn = dc.connectAndInitialize();

        this.channel_manager = new ChannelManager();
        this.runtime = new NodeRuntime(config, channel_manager, conn, dc);
        this.nodeService = new NodeService(runtime);
    }

    public void run() throws IOException, InterruptedException {
        Server server = ServerBuilder.forPort(this.port).addService(nodeService).build().start();

        runtime.setNodeIdentity(this.ip, this.port);
        System.out.println("Node server started on " + this.ip + ":" + this.port);

        initialize();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down node server...");
            server.shutdown();
            seedEx.shutdownNow();
            raftEx.shutdownNow();
            applyEx.shutdownNow();
            channel_manager.closeAllChannels();
        }));

        server.awaitTermination();
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Must provide port as an argument");
            System.exit(1);
        }

        try {
            int port = Integer.parseInt(args[0]);
            new NodeMain(port).run();
        } catch (NumberFormatException e) {
            System.err.println("Invalid port number: " + args[0]);
            System.exit(1);
        } catch (IOException | InterruptedException e) {
            System.err.println("Error starting node server: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private void initialize() {
        System.out.println("Opening channel to seed node...");
        String seedAddr = config.getSeed().getAddress();
        String[] seedParts = seedAddr.split(":");
        channel_manager.openChannel(seedParts[0], Integer.parseInt(seedParts[1]));

        try {
            seed = new Seed(seedParts[0], Integer.parseInt(seedParts[1]));
        } catch (NumberFormatException e) {
            System.err.println("Error parsing seed node address [invalid port]: " + e.getMessage());
            System.exit(1);
        }

        bootstrap_seed_stub = getSeedStub();
        System.out.println("Opened channel to seed node.");

        boolean rejoined = handleRegistration();
        System.out.println("Registration handled.");

        startHeartbeatingToSeed();
        System.out.println("Started heartbeating to seed node.");

        System.out.println("Waiting for partition map to freeze...");
        waitForFrozenPartitionMap();

        System.out.println("Replaying committed entries from persisted Raft state...");
        runtime.replayCommittedEntriesOnStartup();

        if (rejoined) {
            runtime.enterPassiveRejoinMode();
        }

        List<String> addresses = runtime.getPartitionMap().values().stream()
                .flatMap(List::stream)
                .distinct()
                .filter(addr -> !addr.equals(runtime.getNodeId()))
                .collect(Collectors.toList());

        System.out.println("Partition map is frozen. Opening channels to other data nodes.");
        channel_manager.openChannels(addresses);

        pollPartitionMapChanges();
        startElectionLoop();
        startLeaderHeartbeatLoop();
        startApplyLoop();

        System.out.println("Initialization complete. Waiting for incoming requests...");
    }

    private BootstrapSeedGrpc.BootstrapSeedBlockingStub getSeedStub() {
        return channel_manager.getSeedBlocking(seed.ip, seed.port);
    }

    private void startHeartbeatingToSeed() {
        seedEx.scheduleAtFixedRate(() -> {
            try {
                bootstrap_seed_stub.sendHeartbeat(
                        RegisterRequest.newBuilder()
                                .setIp(this.ip)
                                .setPort(this.port)
                                .build());
            } catch (Exception e) {
                System.err.println("Error occurred while heartbeating: " + e.getMessage());
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    private boolean handleRegistration() {
        while (true) {
            RegisterResponse resp = bootstrap_seed_stub.registerAsNode(
                    RegisterRequest.newBuilder()
                            .setIp(this.ip)
                            .setPort(this.port)
                            .build());

            if (resp.getStatus() == RegisterStatus.ACCEPT) {
                System.out.println("Successfully registered as a node.");
                return false;
            } else if (resp.getStatus() == RegisterStatus.REJOIN) {
                System.out.println("Node rejoined. Catch-up will happen if needed.");
                return true;
            } else {
                System.err.println("Registration rejected. Cluster is already frozen.");
                System.exit(1);
            }
        }
    }

    private void waitForFrozenPartitionMap() {
        while (true) {
            PartitionMapResponse resp = bootstrap_seed_stub.getPartitionMap(Empty.getDefaultInstance());

            if (resp.getReady()) {
                Map<Integer, List<String>> partition_map = new ConcurrentHashMap<>();
                for (PartitionSet set : resp.getAssignmentsList()) {
                    partition_map.put(set.getPartition(), new ArrayList<>(set.getNodeIdsList()));
                }
                runtime.updatePartitionMap(partition_map);
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
        raftEx.scheduleAtFixedRate(() -> {
            try {
                PartitionMapResponse resp = bootstrap_seed_stub.getPartitionMap(Empty.getDefaultInstance());

                if (resp.getReady()) {
                    Map<Integer, List<String>> partition_map = new ConcurrentHashMap<>();
                    for (PartitionSet set : resp.getAssignmentsList()) {
                        partition_map.put(set.getPartition(), new ArrayList<>(set.getNodeIdsList()));
                    }
                    runtime.updatePartitionMap(partition_map);
                }
            } catch (Exception e) {
                System.err.println("Error occurred while polling partition map changes: " + e.getMessage());
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    private void startElectionLoop() {
        raftEx.scheduleAtFixedRate(() -> {
            try {
                runtime.runElectionTick();
            } catch (Exception e) {
                System.err.println("Error occurred while running election loop: " + e.getMessage());
            }
        }, 1000, 200, TimeUnit.MILLISECONDS);
    }

    private void startLeaderHeartbeatLoop() {
        raftEx.scheduleAtFixedRate(() -> {
            try {
                runtime.runHeartbeatTick();
            } catch (Exception e) {
                System.err.println("Error occurred while running leadership heartbeat loop: " + e.getMessage());
            }
        }, 0, 500, TimeUnit.MILLISECONDS);
    }

    private void startApplyLoop() {
        applyEx.scheduleAtFixedRate(() -> {
            try {
                runtime.applyCommittedEntries();
            } catch (Exception e) {
                System.err.println("Error occurred while running apply loop: " + e.getMessage());
            }
        }, 1000, 100, TimeUnit.MILLISECONDS);
    }
}