/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */

import com.ericsson.otp.erlang.OtpConnection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.ConnectionFactory;
import de.zib.scalaris.Scalaris;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.examples.CustomOtpFastStringObject;

/**
 * 
 * @author Thibault Dory
 * This class should not be used in it's current state as the java api of Scalaris must be extended to have decent performances
 */

public class scalarisDB extends BenchDB{
	Scalaris sc;
	OtpConnection conn;
	@Override
	public int connectNode(String nodeAddress) {
		int ret;
		try {
			ConnectionFactory cf = new ConnectionFactory(); 
			conn = cf.createConnection("java-client");
			ret = 1;
		} catch (ConnectionException e) {
			e.printStackTrace();
			ret = -1;
		} 
		ConnectionFactory.getInstance().setNode(nodeAddress);
		return ret;
	}

	@Override
	public String readDB(String ID) {
		String res;
		try{
			Transaction transaction = new Transaction(conn);
			transaction.start();
			res = transaction.read(ID);
//			CustomOtpFastStringObject value = new CustomOtpFastStringObject();
//			transaction.readCustom(ID, value);
//			res = value.getValue();
			transaction.commit();
			//transaction.closeConnection();
		}catch (Exception e) {
			e.printStackTrace();
			res = null;
		}
		System.out.println(res);
		return res;
	}

	@Override
	public int updateDB(String ID, String newValue) {
		int res;
		try{
			Transaction transaction = new Transaction(conn);
			transaction.start();
			transaction.writeCustom(ID, new CustomOtpFastStringObject(newValue));
			transaction.commit();
			//transaction.closeConnection();
			res = 1;
		}catch (Exception e) {
			e.printStackTrace();
			res = -1;
		}
		
		return res;
	}

	@Override
	public int writeDB(String ID, String Value) {
		return updateDB(ID,Value);
	}

	@Override
	public void searchDB(String keyword) {
		
	}

}
