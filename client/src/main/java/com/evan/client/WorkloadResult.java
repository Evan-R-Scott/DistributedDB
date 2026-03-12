package com.evan.client;

public class WorkloadResult {
    public final long totalOps;
    public final long success;
    public final long failures;
    public final double avgLatencyMs;
    public final double p50LatencyMs;
    public final double p95LatencyMs;
    public final double p99LatencyMs;
    public final double throughput;
    public final long readOps;
    public final long writeOps;
    public final long deleteOps;

    public WorkloadResult(
            long totalOps,
            long success,
            long failures,
            double avgLatencyMs,
            double p50LatencyMs,
            double p95LatencyMs,
            double p99LatencyMs,
            double throughput,
            long readOps,
            long writeOps,
            long deleteOps) {
        this.totalOps = totalOps;
        this.success = success;
        this.failures = failures;
        this.avgLatencyMs = avgLatencyMs;
        this.p50LatencyMs = p50LatencyMs;
        this.p95LatencyMs = p95LatencyMs;
        this.p99LatencyMs = p99LatencyMs;
        this.throughput = throughput;
        this.readOps = readOps;
        this.writeOps = writeOps;
        this.deleteOps = deleteOps;
    }
}