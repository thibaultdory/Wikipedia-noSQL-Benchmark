package controller;

import java.util.ArrayList;

import utils.Files;

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
		ArrayList<ArrayList<String>> dividedList = divideNodeList(nodeList, clientList.size());
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
		
		int count = 0;
		double sum = 0;
		for(ArrayList<Double> dArray : results){
			for(Double d : dArray){
				count += 1;
				sum += d;
			}
		}
		double squareSum = 0;
		double average = sum / (double) count;
		for(ArrayList<Double> dArray : results){
			for(Double d : dArray){
				squareSum += Math.pow((d - average),2);
			}
		}
		double standardDeviation = Math.sqrt(squareSum / ( (double) (count -1)));
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
	
	static ArrayList<ArrayList<String>> divideNodeList(ArrayList<String> nodeList, int numberOfClients){
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		int numberOfNodes = nodeList.size();
		int low=0;
		int up=numberOfNodes/numberOfClients;
		
		for(int i=0;i<numberOfClients;i++){
			ArrayList<String> local = new ArrayList<String>();
			for(int j=low;j<up;j++){
				if(j<nodeList.size())
					local.add(nodeList.get(j));
				if(j==(nodeList.size()-(numberOfNodes%numberOfClients)-1)){
					for(int u=1;u<=(numberOfNodes%numberOfClients);u++){
						local.add(nodeList.get(j+u));
					}
				}
			}
			result.add(local);
			low = up;
			up += numberOfNodes/numberOfClients;
		}
		return result;
	}

}
