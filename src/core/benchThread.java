package core;
/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */

import java.util.Random;


/**
 * This class does the actual job of the benchmark
 * @author Thibault Dory
 * @version 0.1
 */

public class benchThread extends Thread{
	Random generator = new Random();
	int numberOfOperationsDone = 0;
	int numberOfUpdates =0;
	BenchDB db;
	String nodeAddress;
	int readPercentage;
	int numberOfOperations;
	int numberOfDocuments;

	/**
	 * Constructor of the class
	 * @param dbA is a DB object initialized
	 * @param nodeAddressA is the node address to connect to 
	 * @param readPercentageA is the percentage of requests that will be only reads
	 * @param numberOfOperationsA is the total number of requests that will be made by this thread
	 */
	public benchThread(BenchDB dbA, String nodeAddressA, int readPercentageA, int numberOfOperationsA, int numberOfDocumentsA){
		db = dbA;
		nodeAddress = nodeAddressA;
		readPercentage = readPercentageA;
		numberOfOperations = numberOfOperationsA;
		numberOfDocuments = numberOfDocumentsA;
	}
	/**
	 * This function is called when the thread is started
	 */
	public void run(){
		int countConnectErrors = 0;
		int countReadErrors = 0;
		int countUpdateErrors = 0;
		while(!connect()){
			countConnectErrors += 1;
			System.out.println("This thread cannot connect to : "+nodeAddress+" retrying new attempt : "+countConnectErrors);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		for(int i=0;i<=numberOfOperations;i++){
			//Generate a random ID in the range of the document
			int ID = generator.nextInt(numberOfDocuments)+1;
			String document = db.readDB(String.valueOf(ID));
			if(document == null){
				System.out.println("Thread cannot read for node : "+nodeAddress);
				countReadErrors += 1;
			}
			//Update if randomUpdate > readPercentage to have ~readPercentage % of read only
			int randomUpdate = generator.nextInt(100);
			if(randomUpdate > readPercentage){
				String newDocument = modify(document);
				int  ret = db.updateDB(String.valueOf(ID), newDocument);
				numberOfUpdates += 1;
				if(ret < 0){
					System.out.println("Thread cannot update for node : "+nodeAddress);
					countUpdateErrors += 1;
				}
			}
			numberOfOperationsDone += 1;
		}
		System.out.println("the thread has done "+numberOfOperationsDone+" operations and "+numberOfUpdates+" updates with "+countConnectErrors+" errors");
		runBenchmark.numberOfConnectErrors += countConnectErrors;
		runBenchmark.numberOfReadErrors += countReadErrors;
		runBenchmark.numberOfUpdateErrors += countUpdateErrors;
	}
	
	private boolean connect(){
		boolean ret;
		try{
			int res =  db.connectNode(nodeAddress);
			if(res == 1) ret = true;
			else ret = false;
		}catch(Exception e){
			ret = false;
		}
		return ret;
	}
	/**
	 * Function used to modify the document
	 * @param document the original document
	 * @return the modified document
	 */
	private String modify(String document) {
//		String newDocument = document + "<lorem> Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed adipiscing orci at erat " +
//				"consectetur blandit. Suspendisse feugiat eros nec ante scelerisque et gravida tortor ultrices. Nulla diam felis, " +
//				"vestibulum nec venenatis id, dapibus vitae enim. Nullam porta volutpat ipsum, sit amet sagittis risus dapibus at. " +
//				"Sed diam nibh, eleifend id feugiat vitae, fringilla id odio. Integer est arcu, mattis non vehicula non, dictum sit " +
//				"amet neque. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Maecenas dapibus " +
//				"libero at felis ultricies eleifend. Vivamus luctus blandit libero sed hendrerit. Vestibulum faucibus fermentum turpis, " +
//				"quis dictum risus volutpat vel.Nam dolor lacus, facilisis eget placerat id, rhoncus ut diam. Duis pharetra quam et nibh " +
//				"fermentum et feugiat nulla tincidunt. Pellentesque molestie ligula eget justo pulvinar ut pellentesque mi placerat. " +
//				"Ut at odio vitae nisl pharetra porta ac nec tortor. Class aptent taciti sociosqu ad litora torquent per conubia nostra, " +
//				"per inceptos himenaeos. Maecenas lobortis massa at purus venenatis gravida. Pellentesque consequat ultrices ante, quis " +
//				"dignissim ligula venenatis placerat. Fusce et diam in velit blandit rhoncus vitae sed nibh. Morbi quis libero eros, " +
//				"fermentum vulputate neque. Mauris non felis eget felis ornare porta. In sit amet felis felis. Nunc nisl sapien, " +
//				"gravida eu mollis ut, faucibus vel diam. Fusce blandit dui sed massa rutrum lacinia. Duis eleifend posuere sagittis. " +
//				"Pellentesque vel tellus ligula. Nullam ac dolor vel nisl molestie dictum at vel erat.Fusce sed massa nisi. Curabitur " +
//				"fermentum placerat tortor, a condimentum odio placerat ac. Aenean vitae dolor velit. Vivamus consectetur ante dui, eget " +
//				"dapibus tortor. Suspendisse sed turpis eu felis tincidunt accumsan. Mauris tempor massa id purus placerat pulvinar. </lorem>";
		String newDocument = document + "1";
		return newDocument;
	}
	
}
