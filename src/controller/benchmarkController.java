package controller;

import java.util.ArrayList;

import utils.Files;
import utils.argsTools;
import utils.statTools;

public class benchmarkController {

	public static ArrayList<ArrayList<Double>> results = new ArrayList<ArrayList<Double>>();

	
	public static void main(String[] args) {
		
		//handle arguments
		String dbType = args[0];
		String numberOfOperations = args[1];
		int readPercentage;
		boolean isSearch = false;
		try{
			readPercentage = Integer.decode(args[2]);
		}catch(Exception e){
			readPercentage = 0;
			if(dbType.equals("kill")){
				System.out.println("Sending kill signal to all clients");
			}else{
				e.printStackTrace();
				System.out.println("Bad arguments");
				System.exit(0);
			}
		}
		
		ArrayList<String> clientList = Files.readFileAsList("benchmark_clients");
		ArrayList<String> nodeList = Files.readFileAsList("benchmark_nodes");
		String numberOfOperationsByThread;
		try{
			int temp = Integer.decode(numberOfOperations)/clientList.size();
			numberOfOperationsByThread = String.valueOf(temp);
		}catch(Exception e){
			//search
			numberOfOperationsByThread = "search";
			isSearch = true;
		}
		ArrayList<ArrayList<String>> dividedList = argsTools.divideNodeList(nodeList, clientList.size());
		clientThread t[] = new clientThread[nodeList.size()];
		//start thread for each client
		for(int i=0;i<clientList.size();i++){
			t[i] = new clientThread(clientList.get(i), dbType, numberOfOperationsByThread, readPercentage, dividedList.get(i));
			t[i].start();
			//One thread is enough for the search benchmark
			if(isSearch) break;
		}
		
		if(isSearch){
			try {
				t[0].join();
			} catch (InterruptedException e) {		
				e.printStackTrace();
			}
		}else{
			//Wait until all the threads end for read/update bench
			for(int i=0;i<clientList.size();i++){
				try {
					t[i].join();
				} catch (InterruptedException e) {		
					e.printStackTrace();
				}
			}
		}
		
		statTools stat = new statTools(results);
		double average = stat.getAverage();
		double standardDeviation = stat.getStandardDeviation();
		
		
		if(isSearch){
			System.out.println("Average time and standard deviation taken to build the search index");
			System.out.println("Time in seconds \t Standard deviation");
			System.out.println(average + " \t "+standardDeviation);
		}else{
			System.out.println("Average time and standard deviation taken to complete "+numberOfOperations+" requests");
			System.out.println("Time in seconds \t Standard deviation");
			System.out.println(average + " \t "+standardDeviation);
		}
		

	}

}
