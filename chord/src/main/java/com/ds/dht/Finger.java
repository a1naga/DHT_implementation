package com.ds.dht;

/**
 * A simple bean type class for storing finger address and port.
 */
public class Finger {

    private String address;
    private int port;
    private long nodeId;

    public Finger(String address, int port) {
        this.address = address;
        this.port = port;

        // Hash address:port
        SHAHelper sha1Hasher = new SHAHelper(this.address + ":" + this.port);
        this.nodeId = sha1Hasher.getLong();
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public long getNodeId() {
        return nodeId;
    }

}
