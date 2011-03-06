/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */

package controller;

import java.util.ArrayList;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.w3c.dom.NodeList;

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
		boolean isElasticity = false;
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
		int numberOfDocuments = Integer.decode(args[3]);
		try{
			if(args[4].equals("elasticity")){
				isElasticity = true;
				System.out.println("Starting elasticity test");
			}
		}catch(Exception e){}
		
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
		
		if(! isElasticity){
			startThreads(nodeList, clientList, dbType, numberOfOperationsByThread, readPercentage, numberOfDocuments, dividedList, isSearch);
			
			//The threads results have been added to "results" by clientThread called by startThreads
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
		}else{
			//Get the arguments needed for the elasticity test in the file elasticityArgs
			ArrayList<String> elastArgs = Files.readFileAsList("elasticityArgs");
			double upperLimit = Double.valueOf(elastArgs.get(0));
			double lowerLimit = Double.valueOf(elastArgs.get(1));
			//In milliseconds
			int sleepTime = Integer.valueOf(elastArgs.get(2));
			int maxRuns = Integer.valueOf(elastArgs.get(3));
			//Start the elasticity test
			int countGlobal = 0;
			int countInBounds = 0;
			ArrayList<Double> intermediateResults = new ArrayList<Double>();
			
			while( (countInBounds < 5) && (countGlobal < maxRuns)){
				//Reset the results list
				results = new ArrayList<ArrayList<Double>>();
				startThreads(nodeList, clientList, dbType, numberOfOperationsByThread, readPercentage, numberOfDocuments, dividedList, isSearch);
				statTools stat = new statTools(results);
				double tempSD = stat.getStandardDeviation();
				intermediateResults.add(tempSD);
				System.out.println("Run number "+countGlobal+" has a SD of "+tempSD);
				if((tempSD >= lowerLimit) && (tempSD <= upperLimit)){
					countInBounds += 1;
				}else{
					countInBounds = 0;
				}
				countGlobal += 1;
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			System.out.println("The DB took "+countGlobal+" runs to stabilize with a sleep time of "+sleepTime+" milliseconds");
			System.out.println("Observed standard deviations : ");
			System.out.println(intermediateResults);
		}
		
		
		
		

	}
	
	public static void startThreads(ArrayList<String> nodeList, ArrayList<String> clientList, String dbType, 
			String numberOfOperationsByThread, int readPercentage, int numberOfDocuments, 
			ArrayList<ArrayList<String>> dividedList, boolean isSearch){
		
		clientThread t[] = new clientThread[nodeList.size()];
		//start thread for each client
		for(int i=0;i<clientList.size();i++){
			t[i] = new clientThread(clientList.get(i), dbType, numberOfOperationsByThread, readPercentage, numberOfDocuments, dividedList.get(i));
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
	}

}
