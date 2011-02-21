package implementations;
/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */

import java.io.UnsupportedEncodingException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import core.BenchDB;
import cassandra_mapred.MapReduceCassandraDB;

/**
 * 
 * @author Thibault Dory
 *
 */

public class cassandraDB extends BenchDB{
	final String UTF8 = "UTF8";
	TTransport tr;
	TProtocol proto;
	Cassandra.Client client;
	String keyspace;
    String columnFamily;
    ColumnPath colPathValue; 
    
    public cassandraDB(){
    	keyspace = "Keyspace1";
    	columnFamily = "Standard1";
    	colPathValue = new ColumnPath(columnFamily);
    	try {
			colPathValue.setColumn("value".getBytes(UTF8));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
    }
	
	@Override
	public int connectNode(String nodeAddress) {
		tr = new TSocket(nodeAddress, 9160);
        proto = new TBinaryProtocol(tr);
        client = new Cassandra.Client(proto);
        int ret = 1;
        try {
			tr.open();
		} catch (TTransportException e) {
			e.printStackTrace();
			ret = -1;
		}
		return ret;
	}

	@Override
	public String readDB(String ID) {
		String ret;
		try {
			Column col = client.get(keyspace, ID, colPathValue,ConsistencyLevel.QUORUM).getColumn();
			ret = new String(col.value, UTF8);
		} catch (InvalidRequestException e) {
			e.printStackTrace();
			ret = null;
		} catch (NotFoundException e) {
			e.printStackTrace();
			System.out.println(" ============== was looking for ========  "+ ID +"============");
			ret = null;
		} catch (UnavailableException e) {
			e.printStackTrace();
			ret = null;
		} catch (TimedOutException e) {
			e.printStackTrace();
			ret = null;
		} catch (TException e) {
			e.printStackTrace();
			ret = null;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			ret = null;
		}
		return ret;
	}

	@Override
	public int updateDB(String ID, String newValue) {
		int ret;
		long timestamp = System.currentTimeMillis();
		try {
			client.insert(keyspace, ID, colPathValue, newValue.getBytes(UTF8), timestamp, ConsistencyLevel.QUORUM);
			ret = 1;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			ret = -1;
		} catch (InvalidRequestException e) {
			e.printStackTrace();
			ret = -1;
		} catch (UnavailableException e) {
			e.printStackTrace();
			ret = -1;
		} catch (TimedOutException e) {
			e.printStackTrace();
			ret = -1;
		} catch (TException e) {
			e.printStackTrace();
			ret = -1;
		}
		return ret;
	}

	@Override
	public int writeDB(String ID, String Value) {
		return updateDB(ID, Value);
	}
	@Override
	public void searchDB(String keyword){
		//Replace this by one your Cassandra node's IP
		String[] args = {"192.168.0.1"};
		try {
			MapReduceCassandraDB.main(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
