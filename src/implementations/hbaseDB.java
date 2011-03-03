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
import java.util.UUID;

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
		long t0 = System.nanoTime();
    	
    	
		try {
			//First mapreduce phase setup
	    	HBaseConfiguration conf = config;
	    	conf.set("mapred.job.tracker", "192.168.0.37:8021");
	        Job job;
			job = new Job(conf, "MapReducePhase1");
			job.setJarByClass(MapReduceHbaseDB.class);
	        Scan scan = new Scan();
	        String columns = "myColumnFamily";
	        scan.addColumns(columns);
	        scan.setCaching(10000);
	        
	        //Second mapreduce phase setup
	        HBaseConfiguration conf2 = new HBaseConfiguration();
	        Job job2 = new Job(conf2, "MapReducePhase2");
	        job2.setJarByClass(MapReduceHbaseDB.class);
	        Scan scan2 = new Scan();
	        String columns2 = "resultF"; 
	        scan2.addColumns(columns2);
	        scan2.setCaching(10000);
	        
	        //Execution of the first mapreduce phase
	        TableMapReduceUtil.initTableMapperJob("myTable", scan, Mapper1.class, Text.class,
	                Text.class, job);
	        TableMapReduceUtil.initTableReducerJob("result", Reducer1.class, job);
	        
	        job.waitForCompletion(true);
	        
	        long t2 = System.nanoTime();
	        
	        //Execution of the second mapreduce phase
	        TableMapReduceUtil.initTableMapperJob("result", scan2, Mapper2.class, Text.class,
	                IntWritable.class, job2);
	        TableMapReduceUtil.initTableReducerJob("result2", Reducer2.class, job2);
	        
	        job2.waitForCompletion(true);
	        
	        long t1 = System.nanoTime();
			double totalTime = (t1-t0)/1000000000.0;
			System.out.println("Total time for the search : "+totalTime+" seconds");
	        
			double firstPhaseTime = (t2-t0)/1000000000.0;
			System.out.println("Time for the first mapreduce phase : "+firstPhaseTime+" seconds");
			
			double secondPhaseTime = (t1-t2)/1000000000.0;
			System.out.println("Time for the second mapreduce phase : "+secondPhaseTime+" seconds");

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
	}

}
