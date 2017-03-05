package com.ds.dht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ProtocolHandler implements Runnable {

	private Node currentNode;
	private Socket socket = null;

	public ProtocolHandler(Node node, Socket socket) {
		this.currentNode = node;
		this.socket = socket;
	}

	/**
	 * Method that will read/send messages. It should attempt to read
	 * PING/STORE/FIND_NODE/GET_VALUE messages
	 */
	public void run() {

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

				//System.out.println("Received: " + command + " " + content);

				switch (command) {
				case DHTMain.FIND_VALUE: {
					String response = getValue(content);
					//System.out.println("Sent: " + response);

					// Send response back to client
					socketWriter.println(response);

					break;
				}
				case DHTMain.FIND_NODE: {
					String response = findNode(content);
					//System.out.println("Sent: " + response);

					// Send response back to client
					socketWriter.println(response);

					break;
				}
				case DHTMain.NEW_PREDECESSOR: {
					// Parse address and port from message
					String[] contentFragments = content.split(":");
					String address = contentFragments[0];
					int port = Integer.valueOf(contentFragments[1]);
					System.out.println("GOT NEW PREDECESSOR address : " + address + ", port : " + port);
					// Acquire lock
					currentNode.lock();

					// Move first predecessor to second
					currentNode.setPredecessor2(currentNode.getPredecessor1());

					// Set first predecessor to new finger received in message
					currentNode.setPredecessor1(new Finger(address, port));

					// Release lock
					currentNode.unlock();

					break;
				}
				case DHTMain.REQUEST_PREDECESSOR: {
					// Return the first predecessor address:port
					String response = currentNode.getPredecessor1().getAddress() + ":" + currentNode.getPredecessor1().getPort();
					//System.out.println("Sent: " + response);

					// Send response back to client
					socketWriter.println(response);

					break;
				}
				case DHTMain.PING_QUERY: {
					// Reply to the ping
					String response = DHTMain.PING_RESPONSE;
					//System.out.println("Sent: " + response);

					// Send response back to client
					socketWriter.println(response);

					break;
				}
				/*case DHTMain.PUT_VALUE:{
					String response = putValue(content);
					//System.out.println("Sent: " + response);

					// Send response back to client
					socketWriter.println(response);

					break;
				}*/
				}
			}

			// Close connections
			socketWriter.close();
			socketReader.close();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		//System.out.println("Client connection terminated on port " + socket.getLocalPort());
	}

	private String getValue(String query) {
		// Get long of query
		SHAHelper queryHasher = new SHAHelper(query);
		long queryNodeId = queryHasher.getLong();

		// Wrap the queryNodeId if it is as big as the ring
		if (queryNodeId >= DHTMain.RING_SIZE) {
			queryNodeId -= DHTMain.RING_SIZE;
		}

		String response = "Not found.";

		// If the query is greater than our predecessor id and less than equal
		// to our id then we have the value
		if (isThisMyNode(queryNodeId)) {
			response = "VALUE_FOUND:Request acknowledged on node " + currentNode.getNodeIpAddress() + ":" + currentNode.getPort();
		} else if (isThisNextNode(queryNodeId)) {
			response = "VALUE_FOUND:Request acknowledged on node " + currentNode.getSuccessor1().getAddress() + ":"
					+ currentNode.getSuccessor1().getPort();
		} else { // We don't have the queryNodeId so we must search our fingers for it
			long minimumDistance = DHTMain.RING_SIZE;
			Finger closestPredecessor = null;

			currentNode.lock();

			// Look for a node identifier in the finger table that is less than
			// the queryNodeId and closest in the ID space to the queryNodeId
			for (Finger finger : currentNode.getFingerTable().values()) {
				long distance;

				// Find clockwise distance from finger to queryNodeId
				if (queryNodeId >= finger.getNodeId()) {
					distance = queryNodeId - finger.getNodeId();
				} else {
					distance = queryNodeId + DHTMain.RING_SIZE - finger.getNodeId();
				}

				// If the distance we have found is smaller than the current
				// minimum, replace the current minimum
				if (distance < minimumDistance) {
					minimumDistance = distance;
					closestPredecessor = finger;
				}
			}

			System.out.println("queryNodeId: " + queryNodeId + " minimum distance: " + minimumDistance + " on "
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
						+ closestPredecessor.getPort() + ", position " + " (" + closestPredecessor.getNodeId() + "):");

				response = serverResponse;

				// Close connections
				socketWriter.close();
				socketReader.close();
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			currentNode.unlock();
		}

		return response;
	}

	private String findNode(String query) {
		long queryNodeId = Long.valueOf(query);

		// Wrap the queryid if it is as big as the ring
		if (queryNodeId >= DHTMain.RING_SIZE) {
			queryNodeId -= DHTMain.RING_SIZE;
		}

		String response = "Not found.";

		// If the queryNodeId is greater than our predecessor id and less than equal
		// to our id then we have the value
		if (isThisMyNode(queryNodeId)) {
			response = DHTMain.NODE_FOUND + ":" + currentNode.getNodeIpAddress() + ":" + currentNode.getPort();
		} else if (isThisNextNode(queryNodeId)) {
			response = DHTMain.NODE_FOUND + ":" + currentNode.getSuccessor1().getAddress() + ":"
					+ currentNode.getSuccessor1().getPort();
		} else { // We don't have the query so we must search our fingers for it
			long minimumDistance = DHTMain.RING_SIZE;
			Finger closestPredecessor = null;

			currentNode.lock();

			// Look for a node identifier in the finger table that is less than
			// the key id and closest in the ID space to the key id
			for (Finger finger : currentNode.getFingerTable().values()) {
				long distance;

				// Find clockwise distance from finger to query
				if (queryNodeId >= finger.getNodeId()) {
					distance = queryNodeId - finger.getNodeId();
				} else {
					distance = queryNodeId + DHTMain.RING_SIZE - finger.getNodeId();
				}

				// If the distance we have found is smaller than the current
				// minimum, replace the current minimum
				if (distance < minimumDistance) {
					minimumDistance = distance;
					closestPredecessor = finger;
				}
			}

			System.out.println("queryid: " + queryNodeId + " minimum distance: " + minimumDistance + " on "
					+ closestPredecessor.getAddress() + ":" + closestPredecessor.getPort());

			try {
				// Open socket to chord node
				Socket socket = new Socket(closestPredecessor.getAddress(), closestPredecessor.getPort());

				// Open reader/writer to chord node
				PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

				// Send query to chord
				socketWriter.println(DHTMain.FIND_NODE + ":" + queryNodeId);
				System.out.println("Sent: " + DHTMain.FIND_NODE + ":" + queryNodeId);

				// Read response from chord
				String serverResponse = socketReader.readLine();
				System.out.println("Response from node " + closestPredecessor.getAddress() + ", port "
						+ closestPredecessor.getPort() + ", position " + " (" + closestPredecessor.getNodeId() + "):");

				response = serverResponse;

				// Close connections
				socketWriter.close();
				socketReader.close();
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			currentNode.unlock();
		}

		return response;
	}

	private boolean isThisMyNode(long queryNodeId) {
		boolean response = false;

		// If we are working in a nice clockwise direction without wrapping
		if (currentNode.getNodeId() > currentNode.getPredecessor1().getNodeId()) {
			// If the queryNodeId is between my predecessor and me, the query
			// belongs to me
			if ((queryNodeId > currentNode.getPredecessor1().getNodeId()) && (queryNodeId <= currentNode.getNodeId())) {
				response = true;
			}
		} else { // If we are wrapping
			if ((queryNodeId > currentNode.getPredecessor1().getNodeId()) || (queryNodeId <= currentNode.getNodeId())) {
				response = true;
			}
		}

		return response;
	}

	private boolean isThisNextNode(long queryNodeId) {
		boolean response = false;

		// If we are working in a nice clockwise direction without wrapping
		if (currentNode.getNodeId() < currentNode.getSuccessor1().getNodeId()) {
			// If the query id is between our successor and us, the query
			// belongs to our successor
			if ((queryNodeId > currentNode.getNodeId()) && (queryNodeId <= currentNode.getSuccessor1().getNodeId())) {
				response = true;
			}
		} else { // If we are wrapping
			if ((queryNodeId > currentNode.getNodeId()) || (queryNodeId <= currentNode.getSuccessor1().getNodeId())) {
				response = true;
			}
		}

		return response;
	}

}
