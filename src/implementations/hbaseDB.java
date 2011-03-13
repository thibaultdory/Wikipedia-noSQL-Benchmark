package implementations;
/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */

import hbase_mapreduce.MapReduceHbaseDB;
import hbase_mapreduce.MapReduceHbaseDB.Mapper1;
import hbase_mapreduce.MapReduceHbaseDB.Mapper2;
import hbase_mapreduce.MapReduceHbaseDB.Reducer1;
import hbase_mapreduce.MapReduceHbaseDB.Reducer2;
import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import cassandra_mapreduce.MapReduceCassandraDB;

import core.BenchDB;

/**
 * 
 * @author Thibault Dory
 *
 * Use the following commands in HBase shell to create the needed tables and column families
 * create 'myTable', 'myColumnFamily'
 * create 'result', 'resultF'
 * create 'result2', 'resultF'
 */

public class hbaseDB extends BenchDB{
	HTable table;
	HBaseConfiguration config;
	
	@Override
	public int connectNode(String nodeAddress) {
		int ret;
		config = new HBaseConfiguration();
		try {
			table = new HTable(config, "myTable");
			ret = 1;
		} catch (IOException e) {
			e.printStackTrace();
			ret = -1;
		}
		return ret;
	}
	@Override
	public String readDB(String ID) {
		String ret;
		//The ID is converted to a uuid for performance reasons
		Get g = new Get(Bytes.toBytes(ID));
		try {
			Result r = table.get(g);
			byte [] value = r.getValue(Bytes.toBytes("myColumnFamily"),Bytes.toBytes("value"));
			ret = Bytes.toString(value);
		} catch (IOException e) {
			e.printStackTrace();
			ret = null;
		}
		return ret;
	}

	@Override
	public int updateDB(String ID, String newValue) {
		return writeDB(ID, newValue);
	}
	@Override
	public int writeDB(String ID, String Value) {
		int ret = 0;
		//the row is called ID and is converted into a UUID
		Put p = new Put(Bytes.toBytes(ID));
		try{
			p.add(Bytes.toBytes("myColumnFamily"), Bytes.toBytes("value"), Bytes.toBytes(Value));
			table.put(p);
			ret = 1;
		}catch(Exception e){
			e.printStackTrace();
			ret = -1;			
		}
		return ret;
	}
	@Override
	public void searchDB(String keyword){
		
		String[] args = {"master"};
		try {
			MapReduceHbaseDB.main(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
