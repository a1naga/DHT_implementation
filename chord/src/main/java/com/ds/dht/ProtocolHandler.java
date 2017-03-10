package com.ds.dht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map;

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
			PrintWriter socketWriter = new PrintWriter(
					socket.getOutputStream(), true);
			BufferedReader socketReader = new BufferedReader(
					new InputStreamReader(socket.getInputStream()));

			// Read input from client
			String query;
			while ((query = socketReader.readLine()) != null) {
				// Split the query on the : token in order to get the command
				// and the content portions
				String[] queryContents = query.split(":", 2);
				String command = queryContents[0];
				String content = queryContents[1];

				// System.out.println("Received: " + command + " " + content);

				switch (command) {
				case DHTMain.FIND_VALUE: {
					// Here the content is key got from user
					String response = getValue(content);
					// System.out.println("Sent: " + response);

					// Send response back to client
					System.out.println("GET call output from node "
							+ currentNode.getNodeId() + " " + response);
					socketWriter.println(response);

					break;
				}
				case DHTMain.FIND_NODE: {
					String response = findNode(content);
					// System.out.println("Sent: " + response);

					// Send response back to client
					socketWriter.println(response);

					break;
				}
				case DHTMain.NEW_PREDECESSOR: {
					// Parse address and port from message
					String[] contentFragments = content.split(":");
					String address = contentFragments[0];
					int port = Integer.valueOf(contentFragments[1]);
					System.out.println("GOT NEW PREDECESSOR address : "
							+ address + ", port : " + port);
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
					String response = currentNode.getPredecessor1()
							.getAddress()
							+ ":"
							+ currentNode.getPredecessor1().getPort();
					// System.out.println("Sent: " + response);

					// Send response back to client
					socketWriter.println(response);

					break;
				}
				case DHTMain.PING_QUERY: {
					// Reply to the ping
					String response = DHTMain.PING_RESPONSE;
					// System.out.println("Sent: " + response);

					// Send response back to client
					socketWriter.println(response);

					break;
				}

				case DHTMain.PUT_VALUE: { // System.out.println("Sent: " +
											// response);
					// Send response back to client
					String response = putValue(content);
					socketWriter.println(response);

					break;
				}

				case DHTMain.REQUEST_KEY_VALUES: {
					String response = requestKeyValues(content);
					// System.out.println("Sent from requestKeyValues: " +
					// response);

					// Send response back to client
					socketWriter.println(response);
					break;
				}
				case DHTMain.PUT_REPLICA: {
					// Store replicated data to the given node
					String[] contentFragments = content.split(":");
					String dataKey = contentFragments[2];
					String dataValue = contentFragments[3];
					currentNode.lock();

					// put key,value to dataStore
					currentNode.getDataStore().put(dataKey, dataValue);
					System.out.println("Replicated " + dataKey + "-"
							+ dataValue + " to "
							+ currentNode.getNodeIpAddress() + ":"
							+ currentNode.getPort());

					currentNode.unlock();
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

		// System.out.println("Client connection terminated on port " +
		// socket.getLocalPort());
	}

	private String getValue(String key) {
		// Get long of query
		SHAHelper keyHasher = new SHAHelper(key);
		long hashedKey = keyHasher.getLong();
		System.out.println("Hashed Value for GET --------> " + hashedKey
				+ " my current node id -----> " + currentNode.getNodeId());

		// Wrap the queryNodeId if it is as big as the ring
		if (hashedKey >= DHTMain.RING_SIZE) {
			hashedKey -= DHTMain.RING_SIZE;
		}

		String response = "Key NOT FOUND.";

		// If the query is greater than our predecessor id and less than equal
		// to our id then we have the value
		if (isThisMyNode(hashedKey)) {
			System.out
					.println("isThisMyNode true in GET  current node id ----> "
							+ currentNode.getNodeId());
			String resourceValue = currentNode.getDataStore().get(key);
			if (resourceValue != null && !resourceValue.isEmpty())
				response = "VALUE_FOUND:Request acknowledged on node "
						+ currentNode.getNodeId() + ":" + currentNode.getPort()
						+ ":" + currentNode.getDataStore().get(key);
		}

		/*
		 * else if (isThisNextNode(keyResidingNodeId)) { response =
		 * "VALUE_FOUND:Request acknowledged on node " +
		 * currentNode.getSuccessor1().getAddress() + ":" +
		 * currentNode.getSuccessor1().getPort() + ":" +
		 * currentNode.getDataStore().get(key); }
		 */
		else {
			// We don't have the keyResidingNodeId so we must search our fingers
			// for it
			long minimumDistance = DHTMain.RING_SIZE;
			Finger closestNodeToKey = null;

			currentNode.lock();

			// Look for a node identifier in the finger table that is less than
			// the queryNodeId and closest in the ID space to the queryNodeId
			for (Finger finger : currentNode.getFingerTable().values()) {
				long distance;

				// Find clockwise distance from finger to queryNodeId
				if (finger.getNodeId() >= hashedKey) {
					distance = finger.getNodeId() - hashedKey;
				} else {
					distance = DHTMain.RING_SIZE + finger.getNodeId()
							- hashedKey;
				}

				// If the distance we have found is smaller than the current
				// minimum, replace the current minimum
				if (distance < minimumDistance) {
					minimumDistance = distance;
					closestNodeToKey = finger;
				}
			}

			System.out.println("GET call ---> hashedKey: " + hashedKey
					+ " minimum distance: " + minimumDistance + " on "
					+ closestNodeToKey.getAddress() + ":"
					+ closestNodeToKey.getPort());

			try {
				// Open socket to chord node
				Socket socket = new Socket(closestNodeToKey.getAddress(),
						closestNodeToKey.getPort());

				// Open reader/writer to chord node
				PrintWriter socketWriter = new PrintWriter(
						socket.getOutputStream(), true);
				BufferedReader socketReader = new BufferedReader(
						new InputStreamReader(socket.getInputStream()));

				// Send query to chord
				socketWriter.println(DHTMain.FIND_VALUE + ":" + key);
				// System.out.println("Sent: " + DHTMain.FIND_VALUE + ":" +
				// key);

				// Read response from chord
				String serverResponse = socketReader.readLine();
				System.out.println("GET call ----> Response from node "
						+ closestNodeToKey.getAddress() + ", port "
						+ closestNodeToKey.getPort() + ", position " + " ("
						+ closestNodeToKey.getNodeId() + "):");
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

	private String putValue(String keyValue) {

		String[] keyValueArray = keyValue.split(":");

		// Get long of query
		SHAHelper keyHasher = new SHAHelper(keyValueArray[0]);
		long hashedKey = keyHasher.getLong();

		// Wrap the queryNodeId if it is as big as the ring
		if (hashedKey >= DHTMain.RING_SIZE) {
			hashedKey -= DHTMain.RING_SIZE;
		}

		String response = "Not found.";

		// If the query is greater than our predecessor id and less than equal
		// to our id then we have the value
		if (isThisMyNode(hashedKey)) {

			currentNode.getDataStore().put(keyValueArray[0], keyValueArray[1]);

			response = "VALUE_STORED for " + hashedKey + " on node "
					+ currentNode.getNodeId() + ":" + currentNode.getPort();
		}

		/*
		 * else if (isThisNextNode(keyResidingNodeId)) {
		 * 
		 * currentNode.getDataStore().put(keyValueArray[0], keyValueArray[1]);
		 * 
		 * response = "VALUE_STORED: on node " + currentNode.getNodeId() + ":" +
		 * currentNode.getPort(); }
		 */

		// TODO remove isThisNextNode method below which is not needed

		else {
			// We don't have the keyResidingNodeId so we must search our fingers
			// for it
			long minimumDistance = DHTMain.RING_SIZE;
			Finger closestNodeToKey = null;

			currentNode.lock();

			// Look for a node identifier in the finger table that is less than
			// the keyResidingNodeId and closest in the ID space to the
			// keyResidingNodeId
			for (Finger finger : currentNode.getFingerTable().values()) {
				long distance;

				// Find clockwise distance from finger to queryNodeId
				if (finger.getNodeId() >= hashedKey) {
					distance = finger.getNodeId() - hashedKey;
				} else {
					distance = DHTMain.RING_SIZE + finger.getNodeId()
							- hashedKey;
				}

				// If the distance we have found is smaller than the current
				// minimum, replace the current minimum
				if (distance < minimumDistance) {
					minimumDistance = distance;
					closestNodeToKey = finger;
				}
			}

			System.out.println("keyResidingNodeId: " + hashedKey
					+ " minimum distance: " + minimumDistance + " on "
					+ closestNodeToKey.getAddress() + ":"
					+ closestNodeToKey.getPort());

			try {
				// Open socket to closest node
				Socket socket = new Socket(closestNodeToKey.getAddress(),
						closestNodeToKey.getPort());

				// Open reader/writer to closest node
				PrintWriter socketWriter = new PrintWriter(
						socket.getOutputStream(), true);
				BufferedReader socketReader = new BufferedReader(
						new InputStreamReader(socket.getInputStream()));

				// Send query to chord
				socketWriter.println(DHTMain.PUT_VALUE + ":" + keyValue);
				// System.out.println("Sent: " + DHTMain.PUT_VALUE + ":" +
				// keyValue);

				// Read response from chord
				String serverResponse = socketReader.readLine();
				System.out.println("Response from node "
						+ closestNodeToKey.getAddress() + ", port "
						+ closestNodeToKey.getPort() + ", position " + " ("
						+ closestNodeToKey.getNodeId() + "):");
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

		// If the queryNodeId is greater than our predecessor id and less than
		// equal
		// to our id then we have the value
		if (isThisMyNode(queryNodeId)) {
			response = DHTMain.NODE_FOUND + ":"
					+ currentNode.getNodeIpAddress() + ":"
					+ currentNode.getPort();
		} else if (isThisNextNode(queryNodeId)) {
			response = DHTMain.NODE_FOUND + ":"
					+ currentNode.getSuccessor1().getAddress() + ":"
					+ currentNode.getSuccessor1().getPort();
		} else { // We don't have the query so we must search our fingers for it
			long minimumDistance = DHTMain.RING_SIZE;
			Finger closestNodeToKey = null;

			currentNode.lock();

			// Look for a node identifier in the finger table that is less than
			// the key id and closest in the ID space to the key id
			for (Finger finger : currentNode.getFingerTable().values()) {
				long distance;

				// Find clockwise distance from finger to query
				if (queryNodeId >= finger.getNodeId()) {
					distance = queryNodeId - finger.getNodeId();
				} else {
					distance = queryNodeId + DHTMain.RING_SIZE
							- finger.getNodeId();
				}

				// If the distance we have found is smaller than the current
				// minimum, replace the current minimum
				if (distance < minimumDistance) {
					minimumDistance = distance;
					closestNodeToKey = finger;
				}
			}

			System.out.println("queryid: " + queryNodeId
					+ " minimum distance: " + minimumDistance + " on "
					+ closestNodeToKey.getAddress() + ":"
					+ closestNodeToKey.getPort());

			try {
				// Open socket to chord node
				Socket socket = new Socket(closestNodeToKey.getAddress(),
						closestNodeToKey.getPort());

				// Open reader/writer to chord node
				PrintWriter socketWriter = new PrintWriter(
						socket.getOutputStream(), true);
				BufferedReader socketReader = new BufferedReader(
						new InputStreamReader(socket.getInputStream()));

				// Send query to chord
				socketWriter.println(DHTMain.FIND_NODE + ":" + queryNodeId);
				// System.out.println("Sent: " + DHTMain.FIND_NODE + ":" +
				// queryNodeId);

				// Read response from chord
				String serverResponse = socketReader.readLine();
				System.out.println("Response from node "
						+ closestNodeToKey.getAddress() + ", port "
						+ closestNodeToKey.getPort() + ", position " + " ("
						+ closestNodeToKey.getNodeId() + "):");

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
			if ((queryNodeId > currentNode.getPredecessor1().getNodeId())
					&& (queryNodeId <= currentNode.getNodeId())) {
				response = true;
			}
		} else { // If we are wrapping
			if ((queryNodeId > currentNode.getPredecessor1().getNodeId())
					|| (queryNodeId <= currentNode.getNodeId())) {
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
			if ((queryNodeId > currentNode.getNodeId())
					&& (queryNodeId <= currentNode.getSuccessor1().getNodeId())) {
				response = true;
			}
		} else { // If we are wrapping
			if ((queryNodeId > currentNode.getNodeId())
					|| (queryNodeId <= currentNode.getSuccessor1().getNodeId())) {
				response = true;
			}
		}

		return response;
	}

	private String requestKeyValues(String strNodeId) {
		StringBuffer sbResponse = new StringBuffer();
		long newNodeId = Long.valueOf(strNodeId);
		SHAHelper queryHasher;
		try {
			for (Iterator<Map.Entry<String, String>> it = currentNode
					.getDataStore().entrySet().iterator(); it.hasNext();) {
				Map.Entry<String, String> entry = it.next();
				String strKey = entry.getKey();
				queryHasher = new SHAHelper(strKey);
				long hashedKeyEntry = queryHasher.getLong();
				if (newNodeId < currentNode.getNodeId()
						&& ((hashedKeyEntry > currentNode.getNodeId()) || (hashedKeyEntry < newNodeId))) {
					// sbResponse.append(strKey + ":" + entry.getValue());
					sbResponse.append(strKey + ":" + entry.getValue());
					sbResponse.append("::");

					// Remove the key value pair from the current node
					currentNode.lock();
					it.remove();
					currentNode.unlock();
				} else if ((newNodeId > currentNode.getNodeId())
						&& (hashedKeyEntry > currentNode.getNodeId() && hashedKeyEntry < newNodeId)) {
					sbResponse.append(strKey + ":" + entry.getValue());
					sbResponse.append("::");
					// Remove the key value pair from the current node
					currentNode.lock();
					it.remove();
					currentNode.unlock();
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		String response = sbResponse.toString();
		if (response != null && !response.isEmpty() && response != "")
			response = response.substring(0, response.length() - 2);
		return response;

	}
}