package com.evan.seed;

import java.io.IOException;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import com.evan.core.Config;

public class BootstrapSeedMain {

    private final int port;
    private final Cluster clusterManager;
    private final BootstrapSeedService bss;

    public BootstrapSeedMain(int port) {
        this.port = port;
        this.clusterManager = new Cluster();
        this.bss = new BootstrapSeedService(clusterManager);
    }

    public static void main(String[] args) {
        // if (args.length < 1) {
        // System.err.println("Please provide a port number as an argument");
        // return;
        // }
        String portStr = new Config().load_parameters().getSeed().getNodes().get(0);
        String[] nodeParts = portStr.split(":");
        int port;
        try {
            port = Integer.parseInt(nodeParts[1]);
            // port = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("Invalid port number: " + nodeParts[1]);
            return;
        }

        try {
            new BootstrapSeedMain(port).run();
        } catch (IOException | InterruptedException e) {
            System.err.println("Error starting bootstrap seed server: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    public void run() throws IOException, InterruptedException {
        Server server = ServerBuilder.forPort(port).addService(bss).build().start();
        bss.startMaintenance();
        System.out.println("Bootstrap seed server started on port " + port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down bootstrap seed...");
            bss.shutdown();
            server.shutdown();
        }));

        server.awaitTermination();
    }
}