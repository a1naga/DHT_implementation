package com.ds.dht.query;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import com.ds.dht.SHAHelper;

public class QueryNode {

	private String chordNodeAddress;
	private int chordNodePort;
	private SHAHelper sha1Hasher;

	public QueryNode(String chordNodeAddress, String chordNodePort) {
		this.chordNodeAddress = chordNodeAddress;
		this.chordNodePort = Integer.valueOf(chordNodePort);

		// Hash address
		this.sha1Hasher = new SHAHelper(this.chordNodeAddress + ":" + this.chordNodePort);

		System.out.println("Connection to node " + this.chordNodeAddress + ", port " + this.chordNodePort
				+ ", position " + sha1Hasher.getHex() + " (" + sha1Hasher.getLong() + "):");

		// Establish connection to chord
		this.connectToChord();
	}

	private void connectToChord() {
		try {
			// Open socket to chord node
			Socket socket = new Socket(this.chordNodeAddress, this.chordNodePort);

			// Open reader/writer to chord node
			PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			BufferedReader consoleReader = new BufferedReader(new InputStreamReader(System.in));

			// Prompt for entry on console
			System.out.println("Please enter your search key (or type \"quit\" to leave):");

			// Read from console until quit command
			String key = consoleReader.readLine();
			while (!key.equals("quit")) {
				// Send query to chord
				socketWriter.println("FIND_VALUE:" + key);
				//System.out.println("Sent: " + "FIND_VALUE:" + key);

				// Read response from chord
				String serverResponse = socketReader.readLine();
				// System.out.println("Response from node " +
				// this.chordNodeAddress + ", port " + this.chordNodePort + ",
				// position " + sha1Hasher.getHex() + " (" +
				// sha1Hasher.getLong() + "):");
				System.out.println("Received: " + serverResponse);

				// Prompt for new input
				System.out.println("Please enter your search key (or type \"quit\" to leave):");
				key = consoleReader.readLine();
			}

			// We have now read quit from the console so the program should exit
			socket.close();
			System.exit(0);
		} catch (UnknownHostException e) {
			System.err.println("Error: Unknown host " + this.chordNodeAddress);
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			System.err.println("Error: Cannot connect to host");
			e.printStackTrace();
			System.exit(1);
		}
	}

}
