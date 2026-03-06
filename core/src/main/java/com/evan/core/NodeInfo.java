package com.evan.core;

import java.net.InetSocketAddress;

public class NodeInfo {
    private InetSocketAddress addr;
    private long last_heartbeat;

    public NodeInfo(InetSocketAddress addr, long last_heartbeat) {
        this.addr = addr;
        this.last_heartbeat = last_heartbeat;
    }

    public InetSocketAddress getAddr() {
        return addr;
    }

    public long getLastHeartbeat() {
        return last_heartbeat;
    }

    public void setHeartbeat(long timestamp) {
        this.last_heartbeat = timestamp;
    }

    public String getHost() {
        return addr.getHostString();
    }

    public int getPort() {
        return addr.getPort();
    }

    public String nodeid() {
        return getHost() + ":" + getPort();
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NodeInfo nodeInfo = (NodeInfo) obj;
        return addr.equals(nodeInfo.addr);
    }
}
