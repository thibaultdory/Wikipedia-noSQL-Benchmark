import implementations.cassandraDB;




public class test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		cassandraDB db = new cassandraDB();
		db.connectNode("127.0.0.1");
		System.out.println(db.readDB("345"));

	}

}
