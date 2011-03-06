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

import java.io.IOException;
import utils.Files;

/**
 * Class used to fill in the different databases with the wikipedia data
 * @author Thibault Dory
 * @version 0.1
 */

public class fillDB {

	public static void main(String[] args) {
		
		
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
		
		String basePath = args[1];
		int numberOfInserts = Integer.decode(args[2]);
		String nodeAdress = args[3];
		
		int startID = Integer.valueOf(args[4]);
		int numberOfInsertRun = Integer.valueOf(args[5]);

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

		int retCon = db.connectNode(nodeAdress);
		System.out.println("connection returned value : "+retCon);
		if(retCon > 0){
			for(int j=0;j<numberOfInsertRun;j++){
				for(int i=1;i<=numberOfInserts;i++){
					String xml;
					int ret;
					try {
						xml = Files.readFileAsString(basePath+String.valueOf(i));
						ret = db.writeDB(String.valueOf(i+startID -1), xml);
					} catch (IOException e) {
						e.printStackTrace();
						System.out.println("Cannot read file "+i);
						ret = -1;
					}
					if(ret==-1){
						System.out.println("Insert for file "+i+" failed");
					}
					if(i%200 == 0){
						System.out.println(i+startID-1+" inserts done");
					}
				}
				startID += numberOfInserts; 
			}
		}

	}
	
	

}
