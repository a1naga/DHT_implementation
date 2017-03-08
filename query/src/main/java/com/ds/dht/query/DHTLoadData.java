package com.ds.dht.query;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import com.ds.dht.DHTMain;
import com.ds.dht.Node;
import com.ds.dht.SHAHelper;

public class DHTLoadData {

	private String chordNodeAddress;
	private int chordNodePort;
	private SHAHelper sha1Hasher;
	Node currentNode;

	public DHTLoadData(String address, String port) {
		this.chordNodeAddress = address;
		this.chordNodePort = Integer.valueOf(port);

		// Hash address
		this.sha1Hasher = new SHAHelper(this.chordNodeAddress + ":" + this.chordNodePort);

		System.out.println("Connecting to node " + this.chordNodeAddress + ", port " + this.chordNodePort
				+ ", position " + sha1Hasher.getHex() + " (" + sha1Hasher.getLong() + "):");

		// Establish connection to chord
		connectToChord();

	}

	private void connectToChord() {
		try {
			// Open socket to chord node
			Socket socket = new Socket(this.chordNodeAddress, this.chordNodePort);

			// Open reader/writer to chord node
			PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			try (BufferedReader br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/data.txt")))) {

				String keyValue;
				System.out.println("starting to load data from data.txt");
				while ((keyValue = br.readLine()) != null) {

					socketWriter.println("PUT_VALUE:" + keyValue);
					String response = socketReader.readLine();
					System.out.println(response);
					// TODO check the Response
				}
				System.out.println("loaded data to DHT");

			}
			socketWriter.close();
			socketReader.close();
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

	public static void main(String[] args) {
		// Check arguments
		if (args.length == 2) {
			// Create query node with chord node address and port
			new DHTLoadData(args[0], args[1]);
		} else {
			System.err.println("Usage: DHTLoadData [nodeaddress] [nodeport]");
			System.exit(1);
		}
	}

}
