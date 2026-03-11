package com.evan.client;

public class WorkloadResult {
    public final long totalOps;
    public final long success;
    public final long failures;
    public final double avgLatencyMs;
    public final double throughput;
    public final long readOps;
    public final long writeOps;
    public final long deleteOps;

    public WorkloadResult(
            long totalOps,
            long success,
            long failures,
            double avgLatencyMs,
            double throughput,
            long readOps,
            long writeOps,
            long deleteOps) {
        this.totalOps = totalOps;
        this.success = success;
        this.failures = failures;
        this.avgLatencyMs = avgLatencyMs;
        this.throughput = throughput;
        this.readOps = readOps;
        this.writeOps = writeOps;
        this.deleteOps = deleteOps;
    }
}