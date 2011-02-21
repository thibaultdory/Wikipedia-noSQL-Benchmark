package implementations;
/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */

import core.BenchDB;
import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;

/**
 * 
 * @author Thibault Dory
 * This class should work except for the searchDB function as there is no MapReduce implementation in Voldemort
 */

public class voldermortDB extends BenchDB{
	StoreClient<String, String> client;
	
	@Override
	public int connectNode(String nodeAddress) {
		int ret;
		try{
			String bootstrapUrl = "tcp://"+nodeAddress+":6666";
			StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
	        client = factory.getStoreClient("test");
	        ret = 1;
		}catch(Exception e){
			e.printStackTrace();
			ret = -1;
		}
		return ret;
	}

	@Override
	public String readDB(String ID) {
		String res;
		try{
			 res = client.get(ID).getValue();
		} catch(VoldemortException e) {
			 res = null;
		}
		return res;
	}

	@Override
	public int updateDB(String ID, String newValue) {
		int ret;
		try{
			client.put(ID, newValue);
			ret = 1;
		 } catch(VoldemortException e) {
			ret = -1; 
		 }
		return ret;
	}

	@Override
	public int writeDB(String ID, String Value) {
		return updateDB(ID, Value);
	}

	@Override
	public void searchDB(String keyword) {
		// There is no MapReduce implementation in voldemort
	}

}
