/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */

package client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;

import core.runBenchmark;

public class incomingHandler implements Runnable{
	private Socket socket;
	
	public incomingHandler(Socket socket) {
		this.socket = socket;
		Thread t = new Thread(this);
		t.start();
	}

	public void run() {
		try {
            //Get args from the controller
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			ArrayList<String> argList = (ArrayList<String>) ois.readObject();
			
			if(!(argList.get(0).equals("kill"))){
				//Run the benchmark and get the results
				String dbType = argList.get(0);
				String numberOfOperations = argList.get(1);
				String readPercentage = argList.get(2);
				String numberOfDocuments = argList.get(3);
				String [] args = new String [argList.size()];
				args[0] = dbType;
				args[1] = numberOfOperations;
				args[2] = readPercentage;
				args[3] = numberOfDocuments;
				for(int i=4;i<argList.size();i++){
					args[i] = argList.get(i);
				}
				System.out.println("======= args : "+Arrays.toString(args));
				runBenchmark.main(args);
				//Send the results back
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
	            oos.writeObject(runBenchmark.finalResults);
	            oos.writeObject(runBenchmark.numberOfConnectErrors);
	            oos.writeObject(runBenchmark.numberOfReadErrors);
	            oos.writeObject(runBenchmark.numberOfUpdateErrors);
	            oos.close();
			}
            
            //Close everything
            ois.close();
            socket.close();
            if(argList.get(0).equals("kill")){
            	System.exit(0);
            }
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
