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
	public final static String REQUEST_KEY_VALUES = "REQUEST_KEY_VALUES";
	public final static String PUT_REPLICA="PUT_REPLICA";
	public final static String FIND_VALUE_IN_SUCCESSOR="FIND_VALUE_IN_SUCCESSOR";
	public final static String FIND_LEADER = "FIND_LEADER";
	public final static String ELECT_LEADER = "ELECT_LEADER";
	public final static String LEADER_ELECTED = "LEADER_ELECTED";
	public final static String GET_SUCCESSORS="GET_SUCCESSORS";
	
	public final static long RING_SIZE = 65536;
	public final static int FINGER_TABLE_SIZE = 16;

	public static void main(String[] args) {
		// Check arguments
		//if (args.length == 2) {
		if (args.length == 1) {
			// Create new node
			new Node("127.0.0.1", args[0]);
			//new Node(args[0],args[1]);
		//} else if (args.length == 4) {
		} else if (args.length == 3) {
			// Create new node
			new Node("127.0.0.1", args[0], args[1], args[2]);
			//new Node(args[0], args[1], args[2],args[3]);
		} else {
			System.err.println("Usage: DHTMain [port] || DHTMain [port] [bootStrapNodeAddress] [bootStrapNodePort]");
			System.exit(1);
		}
	}

}
