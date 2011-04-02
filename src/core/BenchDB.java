package core;
/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */


/**
 * Every database client implementation must extends this class and implement its functions
 * @author Thibault Dory
 * @version 0.1
 */
public abstract class BenchDB {
	/**
	 * This function is used to initialize the connection to the node nodeAddress
	 * @param nodeAddress contains the hostname to connect to
	 * @return 1 if everything is ok else return -1
	 */
	public abstract int connectNode(String nodeAddress);
	
	/**
	 * This function is used to get the string corresponding ID from the database
	 * @param ID the key associated to the wanted value
	 * @return the string associated to ID or null if an error occurs
	 */
	public abstract String readDB(String ID);
	
	/**
	 * This function is used to update the value corresponding to ID
	 * @param ID the key
	 * @param newValue the updated value associated to ID
	 * @return 1 if everything goes fine else return -1
	 */
	public abstract int updateDB(String ID, String newValue);
	
	/**
	 * This function is used to write a value corresponding to ID
	 * @param ID the key
	 * @param Value the value associated to ID
	 * @return 1 if everything goes fine else return -1
	 */
	public abstract int writeDB(String ID, String Value);
	
	/**
	 * This function is called to launch the MapReduce build of the reverse index
	 * @param keyword
	 * @return 
	 */
	public abstract void searchDB(String keyword);

	/**
	 * This function is called to clise the connection
	 */
	public abstract void close();
}
