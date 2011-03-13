/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */

package hbase_mapreduce;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * @author Thibault Dory
 * This class implements the two MapReduce phases to build the reverse index
 */

public class MapReduceHbaseDB {

	// TableMapper<KeyOut, ValueOut>
	public static class Mapper1 extends TableMapper<Text, Text> {

		//The value of the keyword is hardcoded for now, I know this is bad, patch welcome :-)
		public String keyword = "location";
        private int numRecords = 0;

        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
        		String id = Bytes.toString(row.get());
        		try{
        			Integer.valueOf(id);
        			Text ID = new Text(id);
            		String article = Bytes.toString(values.getValue(Bytes.toBytes("myColumnFamily"), Bytes.toBytes("value")));
            		String [] words = article.split(" ");
            		for(String w : words){
            			if(w.equalsIgnoreCase(keyword)){
            				Text foundWord = new Text(w);
            				try {
            	                context.write(foundWord,ID);
            	            } catch (InterruptedException e) {
            	                throw new IOException(e);
            	            }
            			}
            		}
        		}catch(Exception e){
        			
        		}
        		
        		
	            numRecords++;
	            if ((numRecords % 10000) == 0) {
	                context.setStatus("mapper processed " + numRecords + " records so far");
	            }
        	
        }
    }

	
	//TableReducer<KeyIn, ValueIn, KeyOut>
    public static class Reducer1 extends TableReducer<Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        	String index = "[";
        	for(Text t : values){
        		index += t.toString() + ",";
        	}
        	index += "]";
            Put put = new Put(Bytes.toBytes("results"));
            put.add(Bytes.toBytes("resultF"), Bytes.toBytes("docsID"), Bytes.toBytes(index));
            context.write(key, put);
        }
    }
    
    public static class Mapper2 extends TableMapper<Text, IntWritable> {

        private int numRecords = 0;

        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
        		String rawList = Bytes.toString(values.getValue(Bytes.toBytes("resultF"), Bytes.toBytes("docsID")));
        		String[] list = rawList.split(",");
        		for(String t : list){
        			try{
        				Integer.valueOf(t);
        				context.write(new Text(t), new IntWritable(1));
        			}catch(Exception e){
        				
        			}
        		}
        		
	            numRecords++;
	            if ((numRecords % 10000) == 0) {
	                context.setStatus("mapper processed " + numRecords + " records so far");
	            }
        	
        }
    }

	
	//TableReducer<KeyIn, ValueIn, KeyOut>
    public static class Reducer2 extends TableReducer<Text, IntWritable, Text> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
        	int sum = 0;
        	for(IntWritable i : values){
        		sum += i.get();
        	}
            Put put = new Put(Bytes.toBytes("results"));
            put.add(Bytes.toBytes("resultF"), Bytes.toBytes(key.toString()), Bytes.toBytes(sum));
            System.out.println(key);
            context.write(key, put);
        }
    }
    
    public static void main(String[] args) throws Exception {
    	long t0 = System.nanoTime();
    	
    	
		try {
			//First mapreduce phase setup
	    	HBaseConfiguration conf = new HBaseConfiguration();
	    	conf.set("mapred.job.tracker", args[0]+":8021");
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
	        job.setOutputFormatClass(TableOutputFormat.class);
	        TableMapReduceUtil.initTableMapperJob("myTable", scan, Mapper1.class, Text.class,
	                Text.class, job);
	        TableMapReduceUtil.initTableReducerJob("result", Reducer1.class, job);
	        
	        job.waitForCompletion(true);
	        
	        long t2 = System.nanoTime();
	        
	        //Execution of the second mapreduce phase
	        job2.setOutputFormatClass(TableOutputFormat.class);
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
