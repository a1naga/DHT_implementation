package com.ds.dht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class Node {

	private String address;
	private int port;

	private String bootStrapNodeAddress = null;
	private int bootStrapNodePort;

	private Finger predecessor2;
	private Finger predecessor1;
	private Finger successor1;
	private Finger successor2;

	private Map<Integer, Finger> fingerTable = new HashMap<>();

	private long id;
	private String hex;
	private Semaphore semaphore = new Semaphore(1);

	/**
	 * Constructor for creating a new Chord node that is the first in the ring.
	 *
	 * @param address
	 *            The address of this node
	 * @param port
	 *            The port that this Chord node needs to listen on
	 */
	public Node(String address, String port) {
		// Set node fields
		this.address = address;
		this.port = Integer.valueOf(port);
		System.out.println("Creating a new Chord ring");
		initialize();
	}

	/**
	 * Constructor for creating a new Chord node that will join an existing
	 * ring.
	 *
	 * @param address
	 *            The address of this node
	 * @param port
	 *            The port that this Chord node needs to listen on
	 * @param bootStrapNodeAddress
	 *            The address of the existing ring member
	 * @param bootStrapNodePort
	 *            The port of the existing ring member
	 */
	public Node(String address, String port, String bootStrapNodeAddress, String bootStrapNodePort) {
		// Set node fields
		this.address = address;
		this.port = Integer.valueOf(port);
		System.out.println("Joining the Chord ring");

		// Set contact node fields
		this.bootStrapNodeAddress = bootStrapNodeAddress;
		this.bootStrapNodePort = Integer.valueOf(bootStrapNodePort);
		System.out.println("Connecting to existing node " + this.bootStrapNodeAddress + ":" + this.bootStrapNodePort);
		initialize();

	}

	private void initialize() {
		// Hash address
		SHAHelper sha1Hasher = new SHAHelper(this.address + ":" + this.port);
		this.id = sha1Hasher.getLong();
		this.hex = sha1Hasher.getHex();

		System.out.println("You are listening on port " + this.port);
		System.out.println("Your position is " + hex + " (" + id + ")");

		// Initialize finger table and successors
		initializeFingers();
		initializeSuccessors();

		// Start listening for connections and heartbeats from neighbors
		new Thread(new NodeServer(this)).start();
		new Thread(new RingStabilizer(this)).start();
		new Thread(new PingHandler(this)).start();
	}

	/**
	 * Initializes finger table. If an existing node has been defined it will
	 * use that node to perform lookups. Otherwise, this node is the only node
	 * in the ring and all fingers will refer to self.
	 */
	private void initializeFingers() {
		// If this ring is the only node in the ring
		if (bootStrapNodeAddress == null) {
			// Initialize all fingers to refer to self
			for (int i = 0; i < 32; i++) {
				fingerTable.put(i, new Finger(address, port));
			}
		} else {
			// Open connection to contact node
			try {
				Socket socket = new Socket(bootStrapNodeAddress, bootStrapNodePort);

				// Open reader/writer to chord node
				PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

				BigInteger bigQuery = BigInteger.valueOf(2L);
				BigInteger bigSelfId = BigInteger.valueOf(id);

				for (int i = 0; i < 32; i++) {
					BigInteger bigResult = bigQuery.pow(i);
					bigResult = bigResult.add(bigSelfId);

					// Send query to chord
					socketWriter.println(DHTMain.FIND_NODE + ":" + bigResult.longValue());
					System.out.println("Sent: " + DHTMain.FIND_NODE + ":" + bigResult.longValue());

					// Read response from chord
					String serverResponse = socketReader.readLine();

					// Parse out address and port
					String[] serverResponseFragments = serverResponse.split(":", 2);
					String[] addressFragments = serverResponseFragments[1].split(":");

					// Add response finger to table
					fingerTable.put(i, new Finger(addressFragments[0], Integer.valueOf(addressFragments[1])));

					System.out.println("Received: " + serverResponse);
				}

				// Close connections
				socketWriter.close();
				socketReader.close();
				socket.close();
			} catch (IOException e) {
				logError("Could not open connection to existing node");
				e.printStackTrace();
			}
		}
	}

	/**
	 * Initializes successors. Uses the finger table to get the successors and
	 * defaults the predecessors to self until it learns about new ones.
	 */
	private void initializeSuccessors() {
		successor1 = fingerTable.get(0);
		successor2 = fingerTable.get(1);
		predecessor1 = new Finger(address, port);
		predecessor2 = new Finger(address, port);

		// Notify the first successor that we are the new predecessor, provided
		// we do not open a connection to ourselves
		if (!address.equals(successor1.getAddress()) || (port != successor1.getPort())) {
			try {
				Socket socket = new Socket(successor1.getAddress(), successor1.getPort());

				// Open writer to successor node
				PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);

				// Tell successor that this node is its new predecessor
				socketWriter.println(DHTMain.NEW_PREDECESSOR + ":" + getAddress() + ":" + getPort());
				System.out.println("Sent: " + DHTMain.NEW_PREDECESSOR + ":" + getAddress() + ":" + getPort()
						+ " to " + successor1.getAddress() + ":" + successor1.getPort());

				// Close connections
				socketWriter.close();
				socket.close();
			} catch (IOException e) {
				logError("Could not open connection to first successor");
				e.printStackTrace();
			}
		}
	}

	/**
	 * Logs error messages to the console
	 *
	 * @param errorMessage
	 *            The message to print to the console
	 */
	private void logError(String errorMessage) {
		System.err.println("Error (" + id + "): " + errorMessage);
	}

	public void lock() {
		try {
			semaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void unlock() {
		semaphore.release();
	}

	public Map<Integer, Finger> getFingerTable() {
		return fingerTable;
	}

	public int getPort() {
		return port;
	}

	public String getAddress() {
		return address;
	}

	public Finger getSuccessor1() {
		return successor1;
	}

	public void setSuccessor1(Finger firstSuccessor) {
		successor1 = firstSuccessor;
	}

	public Finger getPredecessor1() {
		return predecessor1;
	}

	public void setPredecessor1(Finger firstPredecessor) {
		predecessor1 = firstPredecessor;
	}

	public Finger getSuccessor2() {
		return successor2;
	}

	public void setSuccessor2(Finger secondSuccessor) {
		successor2 = secondSuccessor;
	}

	public Finger getPredecessor2() {
		return predecessor2;
	}

	public void setPredecessor2(Finger secondPredecessor) {
		predecessor2 = secondPredecessor;
	}

	public long getId() {
		return id;
	}

	public Semaphore getSemaphore() {
		return semaphore;
	}

}
