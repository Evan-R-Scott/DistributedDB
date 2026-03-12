package com.evan.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

class UserStats {
    long success;
    long failures;
    long latencyNanos;
    long readOps;
    long writeOps;
    long deleteOps;
    List<Long> latenciesNanos = new ArrayList<>();
}

public class WorkloadWorker implements Callable<UserStats> {

    private final Router router;
    private final int ops;
    private final int readPercent;
    private final int keyspace;
    private final Random rand;

    public WorkloadWorker(Router router, int ops, int readPercent, int keyspace, int workerId) {
        this.router = router;
        this.ops = ops;
        this.readPercent = readPercent;
        this.keyspace = keyspace;
        this.rand = new Random(System.nanoTime() + (long) workerId * 9973L);
    }

    @Override
    public UserStats call() {
        UserStats stats = new UserStats();

        for (int i = 0; i < ops; i++) {
            String key = "key" + rand.nextInt(keyspace);
            int roll = rand.nextInt(100);

            long start = System.nanoTime();
            boolean ok;

            if (roll < readPercent) {
                stats.readOps++;
                ok = router.getInternal(key);
            } else if (roll < Math.min(100, readPercent + 10)) {
                stats.deleteOps++;
                ok = router.deleteInternal(key);
            } else {
                stats.writeOps++;
                String value = "value_" + rand.nextInt(1_000_000);
                ok = router.putInternal(key, value);
            }

            long end = System.nanoTime();
            long latency = end - start;

            stats.latencyNanos += latency;
            stats.latenciesNanos.add(latency);

            if (ok) {
                stats.success++;
            } else {
                stats.failures++;
            }
        }

        return stats;
    }
}