package com.evan.core;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ClusterFollower {
    // for now this is of size(1), if time permits convert this to a distributed
    // seed system
    Set<Integer> SUPPORTED_SEED_PORTS = Set.of(8080);

    // seed list defined here but should move it to dockerfile probably for
    // os.get_environ or may need to do differently since using gRPC
    List<InetSocketAddress> seedNodes = new ArrayList<>();

    public ClusterFollower() {
        // may need to change this since using gRPC
        for (Integer port : SUPPORTED_SEED_PORTS) {
            this.seedNodes.add(new InetSocketAddress("localhost", port));
        }
    }
}
