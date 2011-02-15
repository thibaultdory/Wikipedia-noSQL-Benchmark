
public class test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		//BenchDB db = new voldermortDB();
		//BenchDB db = new cassandraDB();
		//BenchDB db = new hbaseDB();
		//BenchDB db = new mongoDB();
		//BenchDB db = new riakDB();
		BenchDB db = new scalarisDB();
		db.connectNode("192.168.0.37");
		db.writeDB("test", "valtest");

	}

}
