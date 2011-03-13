package core;
/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */


import implementations.cassandraDB;
import implementations.hbaseDB;
import implementations.mongoDB;
import implementations.riakDB;
import implementations.scalarisDB;
import implementations.terrastoreDB;
import implementations.voldermortDB;

import java.util.ArrayList;

import javax.print.attribute.standard.NumberOfDocuments;


/**
 * Main class used to start the benchmark
 * @author Thibault Dory
 * @version 0.1
 */


public class runBenchmark {
	static int readPercentage;
	public static ArrayList<Double> finalResults;
	
	public static void main(String[] args) {
		//Handle arguments 
		String dbType = args[0];
		int dbTypeI;
		if(dbType.equals("cassandra")) dbTypeI = 0;
		else if(dbType.equals("scalaris")) dbTypeI = 1;
		else if(dbType.equals("voldemort")) dbTypeI = 2;
		else if(dbType.equals("terrastore")) dbTypeI = 3;
		else if(dbType.equals("riak")) dbTypeI = 4;
		else if(dbType.equals("mongodb")) dbTypeI = 5;
		else if(dbType.equals("hbase")) dbTypeI = 6;
		else dbTypeI = -1;
		
		boolean goodInts;
		int numberOfOperations=0;
		readPercentage=0;
		try{
			readPercentage = Integer.decode(args[2]);
			numberOfOperations = Integer.decode(args[1]);
			goodInts = true;
		}catch(Exception e){
			goodInts = false;
		}

		int numberOfDocuments;
		int startnodeL;
		if(goodInts){
			numberOfDocuments = Integer.decode(args[3]);
			startnodeL = 4;
		}else{
			numberOfDocuments = 1;
			startnodeL = 3;
		}
		
		ArrayList<String> nodeList = new ArrayList<String>();
		for(int i =startnodeL; i<args.length;i++){
			nodeList.add(args[i]);
		}
		//Start the benchmark if the arguments are OK
		if(dbTypeI>=0 && goodInts){
			ArrayList<Long> results = new ArrayList<Long>();
			long t0 = System.nanoTime();
			results.add(startBench(numberOfOperations,nodeList,readPercentage,dbTypeI,numberOfDocuments));
			int runs = 1;
			
			while(runs <= 9){
				results.add(startBench(numberOfOperations,nodeList,readPercentage,dbTypeI,numberOfDocuments));
				runs += 1;
			}
			long t1 = System.nanoTime();
			double totalTime = (t1-t0)/1000000000.0;
			System.out.println("Total time for all runs "+runs+" : "+totalTime);
			ArrayList<Double> resultsInSeconds = new ArrayList<Double>();
			for(int i=0;i<results.size();i++){
				resultsInSeconds.add(i,results.get(i)/ 1000000000.0);
			}
			finalResults = resultsInSeconds;
			System.out.println("Individual times : "+resultsInSeconds);
			
		}else {
			System.out.println("Starting search benchmark");
			ArrayList<Double> res = startBenchSearch(dbTypeI,nodeList);
			finalResults = res;
			System.out.println("Results : "+res);
		}
	}

	
	/**
	 * This function does the actual job of starting all the benchmark thread
	 * @param numberOfOperations
	 * @param nodeList
	 * @param readPercentage
	 * @param dbTypeI 
	 * @return the time taken for all the thread to finish their work in nano seconds
	 */
	public static long startBench(int numberOfOperations, ArrayList<String> nodeList, int readPercentage, int dbTypeI, int numberOfDocuments){
		int numberOfOperationByThread = numberOfOperations/nodeList.size();
		benchThread t[] = new benchThread[nodeList.size()];

		//Start all the threads
		long t0 = System.nanoTime();
		for(int i=0;i<nodeList.size();i++){
			System.out.println("starting thread "+i);
			BenchDB db;
			switch(dbTypeI){
			case 0:
				db = new cassandraDB();
				break;
			case 1:
				db = new scalarisDB();
				break;
			case 2:
				db = new voldermortDB();
				break;
			case 3:
				db = new terrastoreDB();
				break;
			case 4:
				db = new riakDB();
				break;
			case 5:
				db = new mongoDB();
				break;
			case 6:
				db = new hbaseDB();
				break;
			default:
				db = new cassandraDB();
				break;
			}
			
			t[i] = new benchThread(db, nodeList.get(i), readPercentage, numberOfOperationByThread, numberOfDocuments);
			t[i].start();
		}
		
		//Wait until all the threads end
		for(int i=0;i<nodeList.size();i++){
			try {
				t[i].join();
			} catch (InterruptedException e) {		
				e.printStackTrace();
			}
		}
		
		long t1 = System.nanoTime();
        double seconds = (t1 - t0) / 1000000000.0;
        System.out.println("time for "+ numberOfOperations +" requests : " + seconds +" seconds");
		return t1 - t0;
	}
	
	/**
	 * This function calls 
	 * @param dbTypeI
	 * @param nodeList
	 * @return
	 */
	
	public static ArrayList<Double> startBenchSearch(int dbTypeI,ArrayList<String> nodeList){
		BenchDB db;
		switch(dbTypeI){
		case 0:
			db = new cassandraDB();
			break;
		case 1:
			db = new scalarisDB();
			break;
		case 2:
			db = new voldermortDB();
			break;
		case 3:
			db = new terrastoreDB();
			break;
		case 4:
			db = new riakDB();
			break;
		case 5:
			db = new mongoDB();
			break;
		case 6:
			db = new hbaseDB();
			break;	
		default:
			db = new riakDB();
			break;
		}
		db.connectNode(nodeList.get(0));
		ArrayList<Double> Results = new ArrayList<Double>();
		
		//Caution! Here the readPercentage is used as the number of runs for the MapReduce benchmark
		for(int i= 0; i<readPercentage;i++){
			long t0 = System.nanoTime();
			//I have chosen arbitrarily to use this keyword
			//Be carefull it is hardcoded for the HBase implementation for now
			db.searchDB("location");
			long t1 = System.nanoTime();
	        double seconds = (t1 - t0) / 1000000000.0;
	        System.out.println("Search "+i+" took "+seconds+" seconds");
	        Results.add(seconds);
		}
		
		
		return Results;
	}

}
