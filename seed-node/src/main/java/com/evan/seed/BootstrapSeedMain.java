package com.evan.seed;

import java.io.IOException;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class BootstrapSeedMain {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Please provide one of the supported seed port numbers as an argument");
            return;
        }
        // since only 1 seed node, just read port from the config.json
        String port = args[0];
        // String port = "8080"; // hardcoded port for now since starting central seed
        Integer port_int;
        try {
            port_int = Integer.parseInt(port);
        } catch (NumberFormatException e) {
            System.out.println("Invalid port number provided");
            return;
        }
        // track cluster membership and handle updates/requests from router and data
        // nodes
        Cluster cluster_manager = new Cluster();
        BootstrapSeedService bss = new BootstrapSeedService(cluster_manager);

        bss.startMaintenance();
        // Integer port = 8080;

        try {
            Server server = ServerBuilder.forPort(port_int).addService(bss).build().start();
            System.out.println("Bootstrap seed server started on port " + port);
            server.awaitTermination();
        } catch (IOException | InterruptedException e) {
            System.err.println("Error occurred while starting bootstrap seed server: " + e.getMessage());
        }
    }
}
