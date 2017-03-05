package com.ds.dht;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class NodeServer implements Runnable {

    private Node currentNode;

    public NodeServer(Node node) {
        this.currentNode = node;
    }

    public void run() {
        try {
            // Listen for connections on port
            ServerSocket serverSocket = new ServerSocket(currentNode.getPort());

            // Continuously loop for connections
            while (true) {
                // When connection is established launch a new thread for communicating with client
                Socket clientSocket = serverSocket.accept();
                new Thread(new ProtocolHandler(currentNode, clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("error when listening for connections");
            e.printStackTrace();
        }
    }

}
