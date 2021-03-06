package com.ds.dht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

/**
 * The role of RingStabilizer is to keep the finger table up-to-date and to make
 * sure that the predecessor and successor are correct.
 *
 */
public class RingStabilizer extends Thread {

	private Node currentNode;
	private int delaySeconds = 10000;

	public RingStabilizer(Node node) {
		this.currentNode = node;
	}

	/**
	 * Method that periodically runs to determine if node's successor is
	 * uptodate by contacting the listed successor and asking for its
	 * predecessor.
	 */
	public void run() {
		try {
			// Initially sleep
			Thread.sleep(delaySeconds);

			Socket socket = null;
			PrintWriter socketWriter = null;
			BufferedReader socketReader = null;

			while (true) {
				try {
					// Only open a connection to the successor if it is not
					// ourselves
					if (!currentNode.getNodeIpAddress().equals(currentNode.getSuccessor1().getAddress())
							|| (currentNode.getPort() != currentNode.getSuccessor1().getPort())) {
						System.out.println("StabilizeProtocol will run now.");

						// for multiple nodes, successor1 and successor2 cannot
						// be equal
						updateSuccessors();// by priya - initial successor1 & 2
											// updation

						// Open socket to successor
						socket = new Socket(currentNode.getSuccessor1().getAddress(),
								currentNode.getSuccessor1().getPort());

						// Open reader/writer to chord node
						socketWriter = new PrintWriter(socket.getOutputStream(), true);
						socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

						// Submit a request for the predecessor
						socketWriter.println(DHTMain.REQUEST_PREDECESSOR + ":" + currentNode.getNodeId() + " asking "
								+ currentNode.getSuccessor1().getNodeId());
								// System.out.println("Sent: " +
								// DHTMain.REQUEST_PREDECESSOR + ":" +
								// currentNode.getNodeId()
								// + " asking " +
								// currentNode.getSuccessor1().getNodeId());

						// Read response from chord
						String serverResponse = socketReader.readLine();
						// System.out.println("Received: " + serverResponse);

						// Parse server response for address and port
						String[] predecessorFragments = serverResponse.split(":");
						String predecessorAddress = predecessorFragments[0];
						int predecessorPort = Integer.valueOf(predecessorFragments[1]);
						// If the address:port that was returned from the
						// server
						// response is
						// not ourselves then we need to adopt it as our new
						// successor ( Checking if I am still the
						// predecessor
						// for my
						// Successor, if not update my finger table)
						if (!currentNode.getNodeIpAddress().equals(predecessorAddress)
								|| (currentNode.getPort() != predecessorPort)) {
							currentNode.lock();
							Finger newSuccessor = new Finger(predecessorAddress, predecessorPort);

							// Update finger table entries to reflect new
							// successor
							currentNode.getFingerTable().put(1, currentNode.getFingerTable().get(0));
							currentNode.getFingerTable().put(0, newSuccessor);

							currentNode.unlock();
							// Close connections
							socketWriter.close();
							socketReader.close();
							socket.close();

							// Update successor entries to reflect new
							// successor
							updateSuccessors();// by priya

							// If the new node entered has a Higher nodeId than
							// the current LeaderId then start LeaderElection
							System.out.println("Checking if new successor id (" + newSuccessor.getNodeId() + ") > current leader id " + currentNode.getLeaderId());
							startLeaderElectionIfRequired(newSuccessor);

							// Inform new successor that I am your
							// predecessor
							// now
							socket = new Socket(newSuccessor.getAddress(), newSuccessor.getPort());

							// Open writer/reader to new successor node
							socketWriter = new PrintWriter(socket.getOutputStream(), true);
							socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

							// Tell successor that this node is its new
							// predecessor
							socketWriter.println(DHTMain.NEW_PREDECESSOR + ":" + currentNode.getNodeIpAddress() + ":"
									+ currentNode.getPort());
							System.out.println("Sent: " + DHTMain.NEW_PREDECESSOR + ":" + currentNode.getNodeIpAddress()
									+ ":" + currentNode.getPort());

						}

						fingerTableUpdate(socketWriter, socketReader);
						// Close connections
						socketWriter.close();
						socketReader.close();
						socket.close();

						// update successor2 logic//code by priya
						updateSuccessors();

						// manage replicas///code by priya
						// replicate data to successors
						manageReplica();

					}
					// else If I dont have successor entries and if I am not my
					// 1st predecessor
					// then connect to my predecessor and update my finger table
					else if (!currentNode.getNodeIpAddress().equals(currentNode.getPredecessor1().getAddress())
							|| (currentNode.getPort() != currentNode.getPredecessor1().getPort())) {
						// Open socket to successor
						socket = new Socket(currentNode.getPredecessor1().getAddress(),
								currentNode.getPredecessor1().getPort());

						// Open reader/writer to chord node
						socketWriter = new PrintWriter(socket.getOutputStream(), true);
						socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						fingerTableUpdate(socketWriter, socketReader);
						// Close connections
						socketWriter.close();
						socketReader.close();
						socket.close();
						// update successor2 logic//by priya
						updateSuccessors();

						// Happens only in the first node case when a second
						// node joined
						startLeaderElectionIfRequired(currentNode.getSuccessor1());

						// manage replicas///code by priya
						// replicate data to successors
						manageReplica();

					}
				} catch (UnknownHostException e) {
					System.err.println("stabilize() could not find host of first successor");
					e.printStackTrace();
				} catch (IOException e) {// Handles successor node failure
					System.err.println("stabilize() could not connect to first successor");
					e.printStackTrace();
					// which means something is wrong/////////////trying
					// Successor1 connection did not work
					// Contact successor2 to update finger table
					boolean callAgain = true;
					while (callAgain) {
						try {
							// Open socket to successor2
							socket = new Socket(currentNode.getSuccessor2().getAddress(),
									currentNode.getSuccessor2().getPort());
							System.out.println("ask help from successor2 to update finger table:"
									+ currentNode.getSuccessor2().getPort());
							// Open reader/writer to chord node
							socketWriter = new PrintWriter(socket.getOutputStream(), true);
							socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
							// update finger table
							fingerTableUpdate(socketWriter, socketReader);
							// Close connections
							socketWriter.close();
							socketReader.close();
							socket.close();
							updateSuccessors();
							startLeaderElectionIfRequired(currentNode.getSuccessor1());
							System.out.println("successor2 helped me :)");
							callAgain = false;
						} catch (IOException ex) {
							System.out.println("stabilize()-within catch - socket failed");
							ex.printStackTrace();
						} catch (Exception ex) {
							System.out.println("stabilize- within catch - sm excptn:" + ex.getMessage());
							ex.printStackTrace();
						}
						manageReplica();

					}

				}
				// Stabilize again after delay
				Thread.sleep(delaySeconds);
			}
		} catch (InterruptedException e) {
			System.err.println("stabilize() thread interrupted");
			e.printStackTrace();
		} catch (Exception e) {
			System.err.println("stabilize() threw an exception :" + e.getMessage());
			e.printStackTrace();

		}

	}

	// This method is for finger Table updation
	private void fingerTableUpdate(PrintWriter socketWriter, BufferedReader socketReader) throws IOException {
		BigInteger bigQuery = BigInteger.valueOf(2L);
		BigInteger bigSelfId = BigInteger.valueOf(currentNode.getNodeId());

		

		// Update all fingers
		for (int i = 0; i < DHTMain.FINGER_TABLE_SIZE; i++) {
			try {
				BigInteger bigResult = bigQuery.pow(i);
				bigResult = bigResult.add(bigSelfId);

				// Send query to chord
				socketWriter.println(DHTMain.FIND_NODE + ":" + bigResult.longValue());
				// System.out.println("Sent: " + DHTMain.FIND_NODE + ":" +
				// bigResult.longValue());

				// Read response from chord
				String serverResponse = socketReader.readLine();
				System.out.println(serverResponse);
				if (serverResponse != null && !serverResponse.isEmpty()
						&& !serverResponse.equalsIgnoreCase("Not found.")) {
					// Parse out address and port
					String[] serverResponseFragments = serverResponse.split(":", 2);
					String[] addressFragments = serverResponseFragments[1].split(":");
					if (addressFragments != null && addressFragments.length == 2) {
						currentNode.lock();
						// Add response to finger table
						currentNode.getFingerTable().put(i,
								new Finger(addressFragments[0], Integer.valueOf(addressFragments[1])));
						currentNode.setSuccessor1(currentNode.getFingerTable().get(0));
						// currentNode.setSuccessor2(currentNode.getFingerTable().get(1));
						currentNode.unlock();
					}

					// System.out.println("Received: " + serverResponse);
				}

			} catch (Exception e) {
				System.err.println("Error from updateFingerTable(): " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		currentNode.printFingerTableEntries();

		System.out.println("printing data in RingStabilizer " + currentNode.getDataStore());

	}

	// new logic to update successor2//by priya
	private void updateSuccessors() throws UnknownHostException, IOException {
		Socket socket = null;
		PrintWriter socketWriter = null;
		BufferedReader socketReader = null;
		// try {
		currentNode.setSuccessor1(currentNode.getFingerTable().get(0));
		// Open socket to successor
		socket = new Socket(currentNode.getSuccessor1().getAddress(), currentNode.getSuccessor1().getPort());

		// Open reader/writer to chord node
		socketWriter = new PrintWriter(socket.getOutputStream(), true);
		socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

		// socket to successor1 and get its successor1 entry
		BigInteger bigQuery = BigInteger.valueOf(2L);
		BigInteger bigSelfId = BigInteger.valueOf(currentNode.getSuccessor1().getNodeId());
		BigInteger bigResult = bigQuery.pow(0);
		bigResult = bigResult.add(bigSelfId);

		
		// Send query to chord
		socketWriter.println(DHTMain.FIND_NODE + ":" + bigResult.longValue());

		// Read response from chord
		String serverResponse = socketReader.readLine();
		if (serverResponse != null && !serverResponse.isEmpty()) {
			// Parse out address and port
			String[] serverResponseFragments = serverResponse.split(":", 2);
			String[] addressFragments = serverResponseFragments[1].split(":");
			currentNode.lock();
			Finger newSuccessor2 = new Finger(addressFragments[0], Integer.valueOf(addressFragments[1]));
			currentNode.setSuccessor2(newSuccessor2);
			currentNode.unlock();
			System.out.println("On entry:successor1=" + currentNode.getSuccessor1().getPort() + " successor2 set to "
					+ currentNode.getSuccessor2().getPort());
		}
		
		/*
		 * } catch (Exception ex) { System.err.println(
		 * "Error from updateSuccessors():" + ex.getMessage()); }
		 */
		// // Close connections
		// if (socketWriter != null)
		// socketWriter.close();
		try {
			// Close connections
			// if (socketWriter != null)
			socketWriter.close();
			// if (socketReader != null)
			socketReader.close();
			// if (socket != null)
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// replicate data to successor1 and 2.//by priya
	private void manageReplica() {

		// loop through data map entries of this node

		// for (Map.Entry<String, String> entry : currentNode.getDataStore()
		// .entrySet()) {
		for (Iterator<Map.Entry<String, String>> it = currentNode.getDataStore().entrySet().iterator(); it.hasNext();) {
			Map.Entry<String, String> entry = it.next();
			try {
				String dataKey = entry.getKey();
				String dataValue = entry.getValue();
				// Get long of query
				SHAHelper keyHasher = new SHAHelper(dataKey);
				long keyNodeId = keyHasher.getLong();

				// Wrap the queryNodeId if it is as big as the ring
				if (keyNodeId >= DHTMain.RING_SIZE) {
					keyNodeId -= DHTMain.RING_SIZE;
				}
				// If the query is greater than our predecessor id and less than
				// equal
				// to our id then we have the value
				if (isThisMyNode(keyNodeId)) // if(dataKey<=currentNode.getId()
												// &&
												// dataKey>currentNode.getPredecessor1().getNodeId())
				{
					// this node's data

					// if datakey is present in successor2's datamap
					// socket to successor2
					// Store_replica:dataKey:dataValue
					// copy dataKey,dataValue to successor1's datamap
					// close socket
					// check response

					connectToSuccessor(currentNode.getSuccessor1().getAddress(), currentNode.getSuccessor1().getPort(),
							dataKey, dataValue);
					connectToSuccessor(currentNode.getSuccessor2().getAddress(), currentNode.getSuccessor2().getPort(),
							dataKey, dataValue);
				} else {
					// these are replicas -- should i keep?
					// find the owner and check its successor1 and 2
					// if currentnode not a successor, delete dataKey from
					// current node's data store
					if (globalMaintainence(keyNodeId)) {
						currentNode.lock();
						it.remove();
						currentNode.unlock();
					}

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	private boolean globalMaintainence(long keyNodeId) {
		boolean canDelete = false;
		Socket socket = null;
		PrintWriter socketWriter = null;
		BufferedReader socketReader = null;
		try {

			socket = new Socket(currentNode.getSuccessor1().getAddress(), currentNode.getSuccessor1().getPort());
			// Open reader/writer to chord node
			socketWriter = new PrintWriter(socket.getOutputStream(), true);
			socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			// Send query to chord
			socketWriter.println(DHTMain.FIND_NODE + ":" + keyNodeId);

			// Read response from chord
			String address = null;
			int port = 0;
			String serverResponse = socketReader.readLine();
			System.out.println(serverResponse);
			if (serverResponse != null && !serverResponse.isEmpty() && !serverResponse.equalsIgnoreCase("Not found.")) {
				// Parse out address and port
				String[] serverResponseFragments = serverResponse.split(":", 2);
				String[] addressFragments = serverResponseFragments[1].split(":");
				address = addressFragments[0];
				port = Integer.parseInt(addressFragments[1]);
			}
			// Close connections
			socketWriter.close();
			socketReader.close();
			socket.close();

			if (address != null && port != 0) {
				// Open socket to chord node
				socket = new Socket(address, port);

				// Open reader/writer to chord node
				socketWriter = new PrintWriter(socket.getOutputStream(), true);
				socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				// get the successor1 and 2 of this node, if current node is not
				// a successor, delete value from current node
				socketWriter.println(DHTMain.GET_SUCCESSORS + ":" + port);

				String response = socketReader.readLine();
				if (response != null && !response.isEmpty()) {
					String[] responseFragments = response.split(":");
					String successor1NodeId = responseFragments[0];
					String successor2NodeId = responseFragments[1];
					if (!String.valueOf(currentNode.getNodeId()).equals(successor1NodeId)
							&& !String.valueOf(currentNode.getNodeId()).equals(successor2NodeId)) {
						canDelete = true;
					}
				}
				// Close connections
				socketWriter.close();
				socketReader.close();
				socket.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return canDelete;
	}

	// copied it for use in manageReplica
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

	// socket to given node and put replica//by priya
	private void connectToSuccessor(String address, int port, String key, String value) {
		Socket socket = null;
		PrintWriter socketWriter = null;
		// BufferedReader socketReader = null;

		// socket to successor
		try {
			socket = new Socket(address, port);
			// Open reader/writer to chord node
			socketWriter = new PrintWriter(socket.getOutputStream(), true);
			// socketReader = new BufferedReader(new
			// InputStreamReader(socket.getInputStream()));
			// submit a request to copy dataKey,dataValue to successor's datamap
			socketWriter.println(DHTMain.PUT_REPLICA + ":" + address + ":" + port + ":" + key + ":" + value);
			// Read response from chord
			// String serverResponse = socketReader.readLine();
		} catch (Exception ex) {

			System.err.println("Error from connectToSuccessors():" + ex.getMessage() + " when connecting to " + port);
		}

		try {
			// Close connections
			// if (socketWriter != null)
			socketWriter.close();
			// if (socketReader != null)
			// socketReader.close();
			// if (socket != null)
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// check response
		// System.out.println("replicate data to " + address + "-" + port);
		// System.out.println("replicate data to "+address+"-"+port+":
		// "+serverResponse);

	}

	private void startLeaderElectionIfRequired(Finger newSuccessor) {
		if (currentNode.getLeaderId() < newSuccessor.getNodeId()) {
			currentNode.initiateLeaderElection(newSuccessor.getAddress(), newSuccessor.getPort());
		}
	}

}
