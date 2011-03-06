/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */
package client;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class benchClient {

	private ServerSocket server;
    private int port = 6666;

    public benchClient(){
    	try {
			server = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
	public static void main(String[] args) {
		benchClient client = new benchClient();
		client.handleIncoming();
	}
	
	public void handleIncoming(){
		System.out.println("Waiting for message from the controller");
		try {
			Socket socket = server.accept();
			new incomingHandler(socket);
			//Restart the thread to wait for the next command
			this.handleIncoming();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
