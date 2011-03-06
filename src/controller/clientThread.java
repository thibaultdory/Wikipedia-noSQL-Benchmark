/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */

package controller;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;


public class clientThread extends Thread {
	String clientHost;
	ArrayList<String> argList = new ArrayList<String>();
	
	public clientThread(String host, String dbTypeArg, String numberOfOperationsArg,
			int readPercentageArg, int numberOfDocuments, ArrayList<String> nodeListArg){
		clientHost = host;
		argList.add(dbTypeArg);
		argList.add(numberOfOperationsArg);
		argList.add(String.valueOf(readPercentageArg));
		argList.add(String.valueOf(numberOfDocuments));
		for(int i=0;i<nodeListArg.size();i++){
			argList.add(nodeListArg.get(i));
		}
	}
	
	
	public void run(){
		try {
			//Open connection
			InetAddress host = InetAddress.getByName(clientHost);
			Socket socket = new Socket(host.getHostName(),6666);
			//Send arguments
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeObject(argList);
			//Get the answer back
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			ArrayList<Double> clientResult = (ArrayList<Double>) ois.readObject();
			benchmarkController.results.add(clientResult);
			ois.close();
			oos.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (EOFException e){
			System.out.println("Client killed");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
		
	}
}
