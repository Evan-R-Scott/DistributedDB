package com.evan.data;

public class LogEntry {
    public enum OperationType {
        PUT,
        DELETE
    }

    private final int index;
    private final long term;
    private final OperationType op;
    private final String key;
    private final String value;
    private final String requestId;

    public LogEntry(int index, long term, OperationType op, String key, String value, String requestId) {
        this.index = index;
        this.term = term;
        this.op = op;
        this.key = key;
        this.value = value;
        this.requestId = requestId;
    }

    public int getIndex() {
        return index;
    }

    public long getTerm() {
        return term;
    }

    public OperationType getOp() {
        return op;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getRequestId() {
        return requestId;
    }
}
