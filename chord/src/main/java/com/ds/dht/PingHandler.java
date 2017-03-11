package com.ds.dht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class PingHandler implements Runnable {

	private Node currentNode = null;
	private final static int initialDelaySeconds = 30000;
	private final static int delaySeconds = 5000;

	public PingHandler(Node node) {
		this.currentNode = node;
	}

	/**
	 * Sends heartbeats out to neighbors and updates pointers as necessary
	 */
	public void run() {
		try {
			Thread.sleep(PingHandler.initialDelaySeconds);

			while (true) {
				this.pingSuccessor();
				this.pingPredecessor();

				Thread.sleep(PingHandler.delaySeconds);
			}
		} catch (InterruptedException e) {
			System.err.println("checkNeighbors() thread interrupted");
			e.printStackTrace();
		}
	}

	private void pingSuccessor() {
		// Only send heartbeats if we are not the destination
		Finger successor = currentNode.getSuccessor1();
		Socket socket = null;
		PrintWriter socketWriter = null;
		BufferedReader socketReader = null;
		if (!currentNode.getNodeIpAddress().equals(successor.getAddress())
				|| (currentNode.getPort() != successor.getPort())) {
			try {
				// Open socket to successor
				socket = new Socket(successor.getAddress(), successor.getPort());

				// Open reader/writer to chord node
				socketWriter = new PrintWriter(socket.getOutputStream(), true);
				socketReader = new BufferedReader(new InputStreamReader(
						socket.getInputStream()));

				// Send a ping
				socketWriter.println(DHTMain.PING_QUERY + ":"
						+ currentNode.getNodeId());
				// System.out.println("Sent: " + DHTMain.PING_QUERY + ":" +
				// currentNode.getNodeId());

				// Read response
				String serverResponse = socketReader.readLine();
				// System.out.println("Received: " + serverResponse);

				// If we do not receive the proper response then something has
				// gone wrong and we need to set our new immediate successor to
				// the backup
				if (!serverResponse.equals(DHTMain.PING_RESPONSE)) {
					Finger failedSuccessor1 = currentNode.getSuccessor1();
					checkForLeaderDown(failedSuccessor1);
					
					currentNode.lock();
					currentNode.setSuccessor1(currentNode.getSuccessor2());
					currentNode.getFingerTable().put(0, currentNode.getSuccessor2());
					currentNode.unlock();
				}

				// Close connections
				socketWriter.close();
				socketReader.close();
				socket.close();
			} catch (IOException e) {
				System.out.println("pingSuccessor: IOException:" + e.getMessage());
				Finger failedSuccessor1 = currentNode.getSuccessor1();
				checkForLeaderDown(failedSuccessor1);
				
				currentNode.lock();
				System.out.println("setting successor1 to " + currentNode.getSuccessor2().getPort());
				currentNode.setSuccessor1(currentNode.getSuccessor2());
				currentNode.getFingerTable().put(0, currentNode.getSuccessor2());
				currentNode.unlock();
				try {
					if (socket != null) {
						socketWriter.close();
						socketReader.close();
						socket.close();
					}
				} catch (Exception e1) {

				}
			} catch (Exception ex) {
				System.out.println("Exception occurred in pingSuccessor: " + ex.getMessage());
			}
		}
	}

	private void checkForLeaderDown(Finger failedSuccessor1) {
		if (failedSuccessor1.getNodeId() == currentNode.getLeaderId()) {
			System.out.println("**** LEADER DOWN " + failedSuccessor1.getNodeId() + " **** Initiating ELECTION for new leader");
			currentNode.setElectionMessage(DHTMain.ELECT_LEADER);
			initiateLeaderElection(currentNode.getSuccessor2().getAddress(), currentNode.getSuccessor2().getPort());
		}
	}

	private void pingPredecessor() {
		// Only send heartbeats if we are not the destination
		Finger predecessor1 = currentNode.getPredecessor1();
		if (!currentNode.getNodeIpAddress().equals(predecessor1.getAddress())
				|| (currentNode.getPort() != predecessor1.getPort())) {
			try {
				// Open socket to predecessor
				Socket socket = new Socket(predecessor1.getAddress(),
						predecessor1.getPort());

				// Open reader/writer to chord node
				PrintWriter socketWriter = new PrintWriter(
						socket.getOutputStream(), true);
				BufferedReader socketReader = new BufferedReader(
						new InputStreamReader(socket.getInputStream()));

				// Send a ping
				socketWriter.println(DHTMain.PING_QUERY + ":"
						+ currentNode.getNodeId());
				// System.out.println("Sent: " + DHTMain.PING_QUERY + ":" +
				// currentNode.getNodeId());

				// Read response
				String serverResponse = socketReader.readLine();
				// System.out.println("Received: " + serverResponse);

				// If we do not receive the proper response then something has
				// gone wrong and we need to set our new immediate predecessor
				// to the backup
				if (!serverResponse.equals(DHTMain.PING_RESPONSE)) {
					currentNode.lock();
					currentNode.setPredecessor1(currentNode.getPredecessor2());
					currentNode.unlock();
				}

				// Close connections
				socketWriter.close();
				socketReader.close();
				socket.close();
			} catch (IOException e) {
				currentNode.lock();
				currentNode.setPredecessor1(currentNode.getPredecessor2());
				currentNode.unlock();
			} catch (Exception ex) {
				System.out.println("Exception occurred in pingPredecessor: "
						+ ex.getMessage());
			}
		}
	}

	private void initiateLeaderElection(String ipAddress, int port) {
		try {
			// Open socket to predecessor
			Socket socket = new Socket(ipAddress, port);

			// Open reader/writer to chord node
			PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			// Send a ping
			socketWriter.println(DHTMain.ELECT_LEADER + ":" + currentNode.getNodeId());
			// Close connections
			socketWriter.close();
			socketReader.close();
			socket.close();
		} catch (IOException e) {
			currentNode.lock();
			currentNode.setPredecessor1(currentNode.getPredecessor2());
			currentNode.unlock();
		} catch (Exception ex) {
			System.out.println("Exception occurred in pingPredecessor: " + ex.getMessage());
		}

	}

}
