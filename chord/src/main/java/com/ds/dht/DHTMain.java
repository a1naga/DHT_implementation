package com.ds.dht;

public class DHTMain {

	public final static String NEW_PREDECESSOR = "NEW_PREDECESSOR";
	public final static String FIND_VALUE = "FIND_VALUE";
	public final static String FIND_NODE = "FIND_NODE";
	public final static String NODE_FOUND = "NODE_FOUND";
	public final static String REQUEST_PREDECESSOR = "REQUEST_PREDECESSOR";
	public final static String PING_QUERY = "ARE YOU STILL AVAILABLE?";
	public final static String PING_RESPONSE = "YES I AM THERE";
	public final static String PUT_VALUE = "PUT_VALUE";
	public final static long RING_SIZE = 4294967296L;
	public final static int FINGER_TABLE_SIZE = 32;

	public static void main(String[] args) {
		// Check arguments
		if (args.length == 1) {
			// Create new node
			new Node("127.0.0.1", args[0]);
		} else if (args.length == 3) {
			// Create new node
			new Node("127.0.0.1", args[0], args[1], args[2]);
		} else {
			System.err.println("Usage: DHTMain [port] || DHTMain [port] [bootStrapNodeAddress] [bootStrapNodePort]");
			System.exit(1);
		}
	}

}
