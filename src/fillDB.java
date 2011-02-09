import java.io.IOException;
import utils.Files;

/**
 * Class used to fill in the different databases with the wikipedia data
 * @author Thibault Dory
 * @version 0.1
 */

public class fillDB {

	public static void main(String[] args) {
		
		
		String basePath = "/home/tdory/data/";
		int numberOfInserts = 19580;
		//int numberOfInserts = 1000;
		//BenchDB db = new cassandraDB();
		//BenchDB db = new scalarisDB();
		//mongoDB db = new mongoDB(); 
		//riakDB db = new riakDB();
		hbaseDB db =  new hbaseDB();
		int retCon = db.connectNode("192.168.0.37");
		System.out.println("connection returned value : "+retCon);
		if(retCon > 0){
			for(int i=1;i<=numberOfInserts;i++){
				String xml;
				int ret;
				try {
					xml = Files.readFileAsString(basePath+String.valueOf(i));
					ret = db.writeDB(String.valueOf(i), xml);
				} catch (IOException e) {
					e.printStackTrace();
					System.out.println("Cannot read file "+i);
					ret = -1;
				}
				if(ret==-1) break;
				System.out.println("Insert for file "+i+" returned value "+ret);
				
			}
		}

	}
	
	

}
