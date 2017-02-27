package com.ds.dht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ProtocolHandler implements Runnable {

	private Node node;
	private Socket socket = null;

	public ProtocolHandler(Node node, Socket socket) {
		this.node = node;
		this.socket = socket;
	}

	/**
	 * Method that will read/send messages. It should attempt to read
	 * PING/STORE/FIND_NODE/GET_VALUE messages
	 */
	public void run() {
		System.out.println("Client connection established on port " + socket.getLocalPort());

		try {
			// Create readers and writers from socket
			PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			// Read input from client
			String query;
			while ((query = socketReader.readLine()) != null) {
				// Split the query on the : token in order to get the command
				// and the content portions
				String[] queryContents = query.split(":", 2);
				String command = queryContents[0];
				String content = queryContents[1];

				System.out.println("Received: " + command + " " + content);

				switch (command) {
				case DHTMain.FIND_VALUE: {
					String response = getValue(content);
					System.out.println("Sent: " + response);

					// Send response back to client
					socketWriter.println(response);

					break;
				}
				case DHTMain.FIND_NODE: {
					String response = findNode(content);
					System.out.println("Sent: " + response);

					// Send response back to client
					socketWriter.println(response);

					break;
				}
				case DHTMain.NEW_PREDECESSOR: {
					// Parse address and port from message
					String[] contentFragments = content.split(":");
					String address = contentFragments[0];
					int port = Integer.valueOf(contentFragments[1]);

					// Acquire lock
					node.lock();

					// Move fist predecessor to second
					node.setPredecessor2(node.getPredecessor1());

					// Set first predecessor to new finger received in message
					node.setPredecessor1(new Finger(address, port));

					// Release lock
					node.unlock();

					break;
				}
				case DHTMain.REQUEST_PREDECESSOR: {
					// Return the first predecessor address:port
					String response = node.getPredecessor1().getAddress() + ":" + node.getPredecessor1().getPort();
					System.out.println("Sent: " + response);

					// Send response back to client
					socketWriter.println(response);

					break;
				}
				case DHTMain.PING_QUERY: {
					// Reply to the ping
					String response = DHTMain.PING_RESPONSE;
					System.out.println("Sent: " + response);

					// Send response back to client
					socketWriter.println(response);

					break;
				}
				}
			}

			// Close connections
			socketWriter.close();
			socketReader.close();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("Client connection terminated on port " + socket.getLocalPort());
	}

	private String getValue(String query) {
		// Get long of query
		SHAHelper queryHasher = new SHAHelper(query);
		long queryId = queryHasher.getLong();

		// Wrap the queryid if it is as big as the ring
		if (queryId >= DHTMain.RING_SIZE) {
			queryId -= DHTMain.RING_SIZE;
		}

		String response = "Not found.";

		// If the query is greater than our predecessor id and less than equal
		// to our id then we have the value
		if (isThisMyNode(queryId)) {
			response = "VALUE_FOUND:Request acknowledged on node " + node.getAddress() + ":" + node.getPort();
		} else if (isThisNextNode(queryId)) {
			response = "VALUE_FOUND:Request acknowledged on node " + node.getSuccessor1().getAddress() + ":"
					+ node.getSuccessor1().getPort();
		} else { // We don't have the query so we must search our fingers for it
			long minimumDistance = DHTMain.RING_SIZE;
			Finger closestPredecessor = null;

			node.lock();

			// Look for a node identifier in the finger table that is less than
			// the key id and closest in the ID space to the key id
			for (Finger finger : node.getFingerTable().values()) {
				long distance;

				// Find clockwise distance from finger to query
				if (queryId >= finger.getId()) {
					distance = queryId - finger.getId();
				} else {
					distance = queryId + DHTMain.RING_SIZE - finger.getId();
				}

				// If the distance we have found is smaller than the current
				// minimum, replace the current minimum
				if (distance < minimumDistance) {
					minimumDistance = distance;
					closestPredecessor = finger;
				}
			}

			System.out.println("queryid: " + queryId + " minimum distance: " + minimumDistance + " on "
					+ closestPredecessor.getAddress() + ":" + closestPredecessor.getPort());

			try {
				// Open socket to chord node
				Socket socket = new Socket(closestPredecessor.getAddress(), closestPredecessor.getPort());

				// Open reader/writer to chord node
				PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

				// Send query to chord
				socketWriter.println(DHTMain.FIND_VALUE + ":" + query);
				System.out.println("Sent: " + DHTMain.FIND_VALUE + ":" + query);

				// Read response from chord
				String serverResponse = socketReader.readLine();
				System.out.println("Response from node " + closestPredecessor.getAddress() + ", port "
						+ closestPredecessor.getPort() + ", position " + " (" + closestPredecessor.getId() + "):");

				response = serverResponse;

				// Close connections
				socketWriter.close();
				socketReader.close();
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			node.unlock();
		}

		return response;
	}

	private String findNode(String query) {
		long queryId = Long.valueOf(query);

		// Wrap the queryid if it is as big as the ring
		if (queryId >= DHTMain.RING_SIZE) {
			queryId -= DHTMain.RING_SIZE;
		}

		String response = "Not found.";

		// If the query is greater than our predecessor id and less than equal
		// to our id then we have the value
		if (isThisMyNode(queryId)) {
			response = DHTMain.NODE_FOUND + ":" + node.getAddress() + ":" + node.getPort();
		} else if (isThisNextNode(queryId)) {
			response = DHTMain.NODE_FOUND + ":" + node.getSuccessor1().getAddress() + ":"
					+ node.getSuccessor1().getPort();
		} else { // We don't have the query so we must search our fingers for it
			long minimumDistance = DHTMain.RING_SIZE;
			Finger closestPredecessor = null;

			node.lock();

			// Look for a node identifier in the finger table that is less than
			// the key id and closest in the ID space to the key id
			for (Finger finger : node.getFingerTable().values()) {
				long distance;

				// Find clockwise distance from finger to query
				if (queryId >= finger.getId()) {
					distance = queryId - finger.getId();
				} else {
					distance = queryId + DHTMain.RING_SIZE - finger.getId();
				}

				// If the distance we have found is smaller than the current
				// minimum, replace the current minimum
				if (distance < minimumDistance) {
					minimumDistance = distance;
					closestPredecessor = finger;
				}
			}

			System.out.println("queryid: " + queryId + " minimum distance: " + minimumDistance + " on "
					+ closestPredecessor.getAddress() + ":" + closestPredecessor.getPort());

			try {
				// Open socket to chord node
				Socket socket = new Socket(closestPredecessor.getAddress(), closestPredecessor.getPort());

				// Open reader/writer to chord node
				PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

				// Send query to chord
				socketWriter.println(DHTMain.FIND_NODE + ":" + queryId);
				System.out.println("Sent: " + DHTMain.FIND_NODE + ":" + queryId);

				// Read response from chord
				String serverResponse = socketReader.readLine();
				System.out.println("Response from node " + closestPredecessor.getAddress() + ", port "
						+ closestPredecessor.getPort() + ", position " + " (" + closestPredecessor.getId() + "):");

				response = serverResponse;

				// Close connections
				socketWriter.close();
				socketReader.close();
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			node.unlock();
		}

		return response;
	}

	private boolean isThisMyNode(long queryId) {
		boolean response = false;

		// If we are working in a nice clockwise direction without wrapping
		if (node.getId() > node.getPredecessor1().getId()) {
			// If the query id is between our predecessor and us, the query
			// belongs to us
			if ((queryId > node.getPredecessor1().getId()) && (queryId <= node.getId())) {
				response = true;
			}
		} else { // If we are wrapping
			if ((queryId > node.getPredecessor1().getId()) || (queryId <= node.getId())) {
				response = true;
			}
		}

		return response;
	}

	private boolean isThisNextNode(long queryId) {
		boolean response = false;

		// If we are working in a nice clockwise direction without wrapping
		if (node.getId() < node.getSuccessor1().getId()) {
			// If the query id is between our successor and us, the query
			// belongs to our successor
			if ((queryId > node.getId()) && (queryId <= node.getSuccessor1().getId())) {
				response = true;
			}
		} else { // If we are wrapping
			if ((queryId > node.getId()) || (queryId <= node.getSuccessor1().getId())) {
				response = true;
			}
		}

		return response;
	}

}
