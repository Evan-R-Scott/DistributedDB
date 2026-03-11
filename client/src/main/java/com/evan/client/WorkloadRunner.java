package com.evan.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class WorkloadRunner {

    private final Router router;

    public WorkloadRunner(Router router) {
        this.router = router;
    }

    public WorkloadResult run(int users, int opsPerUser, int readPercent, int keyspace) {
        long totalRequestedOps = (long) users * opsPerUser;

        if (totalRequestedOps <= 0) {
            return new WorkloadResult(0, 0, 0, 0.0, 0.0, 0, 0, 0);
        }

        int concurrency = Math.min(
                (int) Math.min(Integer.MAX_VALUE, totalRequestedOps),
                Math.max(8, Runtime.getRuntime().availableProcessors() * 4));

        if (concurrency <= 0) {
            concurrency = 1;
        }

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        List<Future<UserStats>> futures = new ArrayList<>(concurrency);

        long start = System.nanoTime();

        long baseOpsPerWorker = totalRequestedOps / concurrency;
        long extraOps = totalRequestedOps % concurrency;

        for (int i = 0; i < concurrency; i++) {
            int workerOps = (int) (baseOpsPerWorker + (i < extraOps ? 1 : 0));
            if (workerOps <= 0) {
                continue;
            }
            futures.add(pool.submit(new WorkloadWorker(router, workerOps, readPercent, keyspace, i)));
        }

        long totalSuccess = 0;
        long totalFailures = 0;
        long totalLatencyNanos = 0;
        long totalReads = 0;
        long totalWrites = 0;
        long totalDeletes = 0;

        for (Future<UserStats> future : futures) {
            try {
                UserStats stats = future.get();
                totalSuccess += stats.success;
                totalFailures += stats.failures;
                totalLatencyNanos += stats.latencyNanos;
                totalReads += stats.readOps;
                totalWrites += stats.writeOps;
                totalDeletes += stats.deleteOps;
            } catch (Exception e) {
                System.err.println("Worker failed: " + e.getMessage());
            }
        }

        pool.shutdown();

        long end = System.nanoTime();
        long totalOps = totalSuccess + totalFailures;

        double avgLatencyMs = totalOps == 0 ? 0.0 : (totalLatencyNanos / 1_000_000.0) / totalOps;
        double elapsedSeconds = (end - start) / 1_000_000_000.0;
        double throughput = elapsedSeconds <= 0.0 ? 0.0 : totalOps / elapsedSeconds;

        return new WorkloadResult(
                totalOps,
                totalSuccess,
                totalFailures,
                avgLatencyMs,
                throughput,
                totalReads,
                totalWrites,
                totalDeletes);
    }
}