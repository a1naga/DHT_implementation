package com.ds.dht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * The role of RingStabilizer is to keep the finger table up-to-date and to make sure that the predecessor and successor 
 * are correct.
 *
 */
public class RingStabilizer extends Thread {

	private Node currentNode;
	private int delaySeconds = 10000;

	public RingStabilizer(Node node) {
		this.currentNode = node;
	}

	/**
	 * Method that periodically runs to determine if node's successor is uptodate
	 * by contacting the listed successor and asking for its predecessor. 
	 */
	public void run() {
		try {
			// Initially sleep
			Thread.sleep(delaySeconds);

			Socket socket = null;
			PrintWriter socketWriter = null;
			BufferedReader socketReader = null;

			while (true) {
				// Only open a connection to the successor if it is not
				// ourselves
				if (!currentNode.getNodeIpAddress().equals(currentNode.getSuccessor1().getAddress())
						|| (currentNode.getPort() != currentNode.getSuccessor1().getPort())) {
					// Open socket to successor
					socket = new Socket(currentNode.getSuccessor1().getAddress(),
							currentNode.getSuccessor1().getPort());

					// Open reader/writer to chord node
					socketWriter = new PrintWriter(socket.getOutputStream(), true);
					socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

					// Submit a request for the predecessor
					socketWriter.println(DHTMain.REQUEST_PREDECESSOR + ":" + currentNode.getNodeId() + " asking "
							+ currentNode.getSuccessor1().getNodeId());
					//System.out.println("Sent: " + DHTMain.REQUEST_PREDECESSOR + ":" + currentNode.getNodeId()
					//		+ " asking " + currentNode.getSuccessor1().getNodeId());

					// Read response from chord
					String serverResponse = socketReader.readLine();
					//System.out.println("Received: " + serverResponse);

					// Parse server response for address and port
					String[] predecessorFragments = serverResponse.split(":");
					String predecessorAddress = predecessorFragments[0];
					int predecessorPort = Integer.valueOf(predecessorFragments[1]);

					// If the address:port that was returned from the server response is
					// not ourselves then we need to adopt it as our new
					// successor ( Checking if I am still the predecessor for my Successor, if not update my finger table)
					if (!currentNode.getNodeIpAddress().equals(predecessorAddress)
							|| (currentNode.getPort() != predecessorPort)) {
						currentNode.lock();

						Finger newSuccessor = new Finger(predecessorAddress, predecessorPort);

						// Update finger table entries to reflect new successor
						currentNode.getFingerTable().put(1, currentNode.getFingerTable().get(0));
						currentNode.getFingerTable().put(0, newSuccessor);

						// Update successor entries to reflect new successor
						currentNode.setSuccessor2(currentNode.getFingerTable().get(1));
						currentNode.setSuccessor1(currentNode.getFingerTable().get(0));
						currentNode.unlock();

						// Close connections
						socketWriter.close();
						socketReader.close();
						socket.close();

						// Inform new successor that I am your predecessor now
						socket = new Socket(newSuccessor.getAddress(), newSuccessor.getPort());

						// Open writer/reader to new successor node
						socketWriter = new PrintWriter(socket.getOutputStream(), true);
						socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

						// Tell successor that I am your new predecessor
						socketWriter.println(DHTMain.NEW_PREDECESSOR + ":" + currentNode.getNodeIpAddress() + ":"
								+ currentNode.getPort());
						//System.out.println("Sent: " + DHTMain.NEW_PREDECESSOR + ":" + currentNode.getNodeIpAddress()
						//		+ ":" + currentNode.getPort());
						
						//Redistribute key value pair from new successor to itself
                        socketWriter.println(DHTMain.REQUEST_KEY_VALUES + ":" + currentNode.getNodeId());
        				serverResponse = socketReader.readLine();
        				if (serverResponse != null && !serverResponse.isEmpty()
        						&& serverResponse != "") {
        					String[] keyValuePairs = serverResponse.split("::");
        					currentNode.lock();

        					for (int i = 0; i < keyValuePairs.length; i++) {
        						String[] keyValue = keyValuePairs[i].split(":", 2);
        						if (keyValue.length == 2) {
        							String key = keyValue[0];
        							String value = keyValue[1];
        							currentNode.getDataStore().put(key, value);
        							System.out.println("(key,value) => (" + key+ ","+value+")" + "sent to :" + currentNode.getNodeIpAddress() + ":" + currentNode.getPort());
        						}
        					}
					}
					fingerTableUpdate(socketWriter,socketReader);
					// Close connections
					socketWriter.close();
					socketReader.close();
					socket.close();
					
					
				} 
				}
				// else If I dont have successor entries and if I am not my 1st predecessor then connect to my predecessor and update my finger table
				else if (!currentNode.getNodeIpAddress().equals(currentNode.getPredecessor1().getAddress())
						|| (currentNode.getPort() != currentNode.getPredecessor1().getPort())) {
					
					// Open socket to predecessor
					socket = new Socket(currentNode.getPredecessor1().getAddress(),
							currentNode.getPredecessor1().getPort());

					// Open reader/writer to chord node
					socketWriter = new PrintWriter(socket.getOutputStream(), true);
					socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					fingerTableUpdate(socketWriter,socketReader);
					// Close connections
					socketWriter.close();
					socketReader.close();
					socket.close();
					
				}
				
				// Stabilize again after delay
				Thread.sleep(delaySeconds);
			}
			
		} catch (InterruptedException e) {
			System.err.println("stabilize() thread interrupted");
			e.printStackTrace();
		} catch (UnknownHostException e) {
			System.err.println("stabilize() could not find host of first successor");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("stabilize() could not connect to first successor");
			e.printStackTrace();
		}
	}
	
	// This method is for finger Table updation
	private void fingerTableUpdate(PrintWriter socketWriter, BufferedReader socketReader) throws IOException{
		BigInteger bigQuery = BigInteger.valueOf(2L);
		BigInteger bigSelfId = BigInteger.valueOf(currentNode.getNodeId());

		currentNode.lock();

		// Update all fingers
		for (int i = 0; i < DHTMain.FINGER_TABLE_SIZE; i++) {
			BigInteger bigResult = bigQuery.pow(i);
			bigResult = bigResult.add(bigSelfId);

			// Send query to chord
			socketWriter.println(DHTMain.FIND_NODE + ":" + bigResult.longValue());
			//System.out.println("Sent: " + DHTMain.FIND_NODE + ":" + bigResult.longValue());

			// Read response from chord
			String serverResponse = socketReader.readLine();

			// Parse out address and port
			String[] serverResponseFragments = serverResponse.split(":", 2);
			String[] addressFragments = serverResponseFragments[1].split(":");

			// Add response to finger table
			currentNode.getFingerTable().put(i,
					new Finger(addressFragments[0], Integer.valueOf(addressFragments[1])));
			currentNode.setSuccessor1(currentNode.getFingerTable().get(0));
			currentNode.setSuccessor2(currentNode.getFingerTable().get(1));

			//System.out.println("Received: " + serverResponse);
		}
		currentNode.unlock();
		currentNode.printFingerTableEntries();
		
	}
	
	

}
