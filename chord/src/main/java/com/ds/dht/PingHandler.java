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
        if (!currentNode.getNodeIpAddress().equals(successor.getAddress()) || (currentNode.getPort() != successor.getPort())) {
            try {
                // Open socket to successor
                Socket socket = new Socket(successor.getAddress(), successor.getPort());

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send a ping
                socketWriter.println(DHTMain.PING_QUERY + ":" + currentNode.getNodeId());
                //System.out.println("Sent: " + DHTMain.PING_QUERY + ":" + currentNode.getNodeId());

                // Read response
                String serverResponse = socketReader.readLine();
                //System.out.println("Received: " + serverResponse);

                // If we do not receive the proper response then something has gone wrong and we need to set our new immediate successor to the backup
                if (!serverResponse.equals(DHTMain.PING_RESPONSE)) {
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
                currentNode.lock();
                currentNode.setSuccessor1(currentNode.getSuccessor2());
                currentNode.getFingerTable().put(0, currentNode.getSuccessor2());
                currentNode.unlock();
            }
        }
    }

    private void pingPredecessor() {
        // Only send heartbeats if we are not the destination
		Finger predecessor1 = currentNode.getPredecessor1();
        if (!currentNode.getNodeIpAddress().equals(predecessor1.getAddress()) || (currentNode.getPort() != predecessor1.getPort())) {
            try {
                // Open socket to predecessor
                Socket socket = new Socket(predecessor1.getAddress(), predecessor1.getPort());

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send a ping
                socketWriter.println(DHTMain.PING_QUERY + ":" + currentNode.getNodeId());
                //System.out.println("Sent: " + DHTMain.PING_QUERY + ":" + currentNode.getNodeId());

                // Read response
                String serverResponse = socketReader.readLine();
                //System.out.println("Received: " + serverResponse);

                // If we do not receive the proper response then something has gone wrong and we need to set our new immediate predecessor to the backup
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
            }
        }
    }

}
