/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */

package core;

import client.benchClient;
import controller.benchmarkController;

/*
 * This class is a simple entry point for the other main classes
 */

public class Main {

	public static void main(String[] args) {
		String [] newArgs = new String[args.length -1];
		for(int i=0; i<(args.length -1);i++){
			newArgs[i] = args[i+1];
		}
		if(args[0].equals("runBenchmark")){
			runBenchmark.main(newArgs);
		}else if(args[0].equals("controller")){
			benchmarkController.main(newArgs);
		}else if(args[0].equals("client")){
			benchClient.main(newArgs);
		}else if(args[0].equals("fillDB")){
			fillDB.main(newArgs);
		}else if(args[0].equals("verifyAndCorrect")){
			verifyAndCorrect.main(newArgs);
		}

	}

}
