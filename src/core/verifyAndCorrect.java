package core;

import java.io.IOException;

import utils.Files;
import implementations.cassandraDB;
import implementations.hbaseDB;
import implementations.mongoDB;
import implementations.riakDB;
import implementations.scalarisDB;
import implementations.terrastoreDB;
import implementations.voldermortDB;

public class verifyAndCorrect {

	/**
	 * @param args
	 */
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
		int numberOfDocuments = Integer.decode(args[2]);
		int startID = Integer.decode(args[3]);
		String nodeAdress = args[4];

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
			for(int i=1+startID;i<=numberOfDocuments;i++){
				String xml="";
				try {
					xml = Files.readFileAsString(basePath+String.valueOf(i));
				} catch (IOException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				try{
					String ret = db.readDB(String.valueOf(i));
					if(ret == null){
						throw new Exception();
					}
					db.updateDB(String.valueOf(i), xml);
				}catch(Exception e){
					db.writeDB(String.valueOf(i), xml);
					System.out.println("Corrected ID : "+i);
				}
				if(i%200 == 0){
					System.out.println(i+" verifications done");
				}
			}
		}
	}

}
