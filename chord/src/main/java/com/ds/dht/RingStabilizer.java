package com.ds.dht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;

public class RingStabilizer extends Thread {

    private Node node;
    private int delaySeconds = 10000;

    public RingStabilizer(Node node) {
        this.node = node;
    }

    /**
     * Method that periodically runs to determine if node needs a new successor by contacting the listed successor and asking for its predecessor. If the current successor has a predecessor that is different than itself it sets its successor to the predecessor.
     */
    public void run() {
        try {
            // Initially sleep
            Thread.sleep(delaySeconds);

            Socket socket = null;
            PrintWriter socketWriter = null;
            BufferedReader socketReader = null;

            while (true) {
                // Only open a connection to the successor if it is not ourselves
                if (!node.getAddress().equals(node.getSuccessor1().getAddress()) || (node.getPort() != node.getSuccessor1().getPort())) {
                    // Open socket to successor
                    socket = new Socket(node.getSuccessor1().getAddress(), node.getSuccessor1().getPort());

                    // Open reader/writer to chord node
                    socketWriter = new PrintWriter(socket.getOutputStream(), true);
                    socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    // Submit a request for the predecessor
                    socketWriter.println(DHTMain.REQUEST_PREDECESSOR + ":" + node.getId() + " asking " + node.getSuccessor1().getId());
                    System.out.println("Sent: " + DHTMain.REQUEST_PREDECESSOR + ":" + node.getId() + " asking " + node.getSuccessor1().getId());

                    // Read response from chord
                    String serverResponse = socketReader.readLine();
                    System.out.println("Received: " + serverResponse);

                    // Parse server response for address and port
                    String[] predecessorFragments = serverResponse.split(":");
                    String predecessorAddress = predecessorFragments[0];
                    int predecessorPort = Integer.valueOf(predecessorFragments[1]);

                    // If the address:port that was returned from the server is not ourselves then we need to adopt it as our new successor
                    if (!node.getAddress().equals(predecessorAddress) || (node.getPort() != predecessorPort)) {
                        node.lock();

                        Finger newSuccessor = new Finger(predecessorAddress, predecessorPort);

                        // Update finger table entries to reflect new successor
                        node.getFingerTable().put(1, node.getFingerTable().get(0));
                        node.getFingerTable().put(0, newSuccessor);

                        // Update successor entries to reflect new successor
                        node.setSuccessor2(node.getSuccessor1());
                        node.setSuccessor1(newSuccessor);

                        node.unlock();

                        // Close connections
                        socketWriter.close();
                        socketReader.close();
                        socket.close();

                        // Inform new successor that we are now their predecessor
                        socket = new Socket(newSuccessor.getAddress(), newSuccessor.getPort());

                        // Open writer/reader to new successor node
                        socketWriter = new PrintWriter(socket.getOutputStream(), true);
                        socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        // Tell successor that this node is its new predecessor
                        socketWriter.println(DHTMain.NEW_PREDECESSOR + ":" + node.getAddress() + ":" + node.getPort());
                        System.out.println("Sent: " + DHTMain.NEW_PREDECESSOR + ":" + node.getAddress() + ":" + node.getPort());
                    }

                    BigInteger bigQuery = BigInteger.valueOf(2L);
                    BigInteger bigSelfId = BigInteger.valueOf(node.getId());

                    node.lock();

                    // Refresh all fingers by asking successor for nodes
                    for (int i = 0; i < 32; i++) {
                        BigInteger bigResult = bigQuery.pow(i);
                        bigResult = bigResult.add(bigSelfId);

                        // Send query to chord
                        socketWriter.println(DHTMain.FIND_NODE + ":" + bigResult.longValue());
                        System.out.println("Sent: " + DHTMain.FIND_NODE + ":" + bigResult.longValue());

                        // Read response from chord
                        serverResponse = socketReader.readLine();

                        // Parse out address and port
                        String[] serverResponseFragments = serverResponse.split(":", 2);
                        String[] addressFragments = serverResponseFragments[1].split(":");

                        // Add response finger to table
                        node.getFingerTable().put(i, new Finger(addressFragments[0], Integer.valueOf(addressFragments[1])));
                        node.setSuccessor1(node.getFingerTable().get(0));
                        node.setSuccessor2(node.getFingerTable().get(1));

                        System.out.println("Received: " + serverResponse);
                    }

                    node.unlock();

                    // Close connections
                    socketWriter.close();
                    socketReader.close();
                    socket.close();
                } else if (!node.getAddress().equals(node.getPredecessor1().getAddress()) || (node.getPort() != node.getPredecessor1().getPort())) {
                    // Open socket to successor
                    socket = new Socket(node.getPredecessor1().getAddress(), node.getPredecessor1().getPort());

                    // Open reader/writer to chord node
                    socketWriter = new PrintWriter(socket.getOutputStream(), true);
                    socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    BigInteger bigQuery = BigInteger.valueOf(2L);
                    BigInteger bigSelfId = BigInteger.valueOf(node.getId());

                    node.lock();

                    // Refresh all fingers by asking successor for nodes
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
                        node.getFingerTable().put(i, new Finger(addressFragments[0], Integer.valueOf(addressFragments[1])));
                        node.setSuccessor1(node.getFingerTable().get(0));
                        node.setSuccessor2(node.getFingerTable().get(1));

                        System.out.println("Received: " + serverResponse);
                    }

                    node.unlock();

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

}
