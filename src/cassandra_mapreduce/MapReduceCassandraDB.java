/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */

package cassandra_mapreduce;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import org.apache.log4j.Logger;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.avro.Column;
import org.apache.cassandra.avro.ColumnOrSuperColumn;
import org.apache.cassandra.avro.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.Files;

/** 
 * @author Thibault Dory
 */

public class MapReduceCassandraDB extends Configured implements Tool
{
    private static final Logger logger = Logger.getLogger(MapReduceCassandraDB.class);

    static final String KEYSPACE = "Keyspace1";
    static final String COLUMN_FAMILY = "Standard1";
    private static final String CONF_COLUMN_NAME = "value";
    private static final String OUTPUT_PATH_PREFIX = "/tmp/wikipedia";
    static final int RING_DELAY = 30000; // this is enough for testing a single server node; may need more for a real cluster
    static final String OUTPUT_COLUMN_FAMILY = "output_phase1";
    static final String OUTPUT_COLUMN_FAMILY2 = "output_phase2";

    public static void main(String[] args) throws Exception
    {
    	//Delete old results if any
    	Files.deleteDirectory(new File(OUTPUT_PATH_PREFIX));
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new MapReduceCassandraDB(), args);
    }

    public static class TokenizerMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, Text>
    {
    	//Here is the ugly hardcoded keyword
        public String keyword = "location";
        private ByteBuffer sourceColumn;
        
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException
        {
        	sourceColumn = ByteBuffer.wrap(context.getConfiguration().get(CONF_COLUMN_NAME).getBytes());
        }

        public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
        {
            IColumn column = columns.get(sourceColumn);
            if (column == null)
                return;
            String value;
            try{
            	value = ByteBufferUtil.string(column.value());
            }catch(Exception e){
            	System.out.println("crash for key : "+ByteBufferUtil.string(key));
            	return;
            }
            String [] words = value.split(" ");
            Text ID = new Text(ByteBufferUtil.string(key));
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
        }
        
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text>
    {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            String index = "[";
        	for(Text t : values){
        		index += t.toString() + ",";
        	}
        	index += "]";
            Text res = new Text(index);
            context.write(key, res);
        }
    }
    
    public static class ReducerToCassandra extends Reducer<Text, Text, ByteBuffer,List<Mutation>>
    {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
        	String index = "";
        	for(Text t : values){
        		index += t.toString() + ",";
        	}
        	index += "";
            context.write(ByteBuffer.wrap(key.toString().getBytes()), Collections.singletonList(getMutation(index)));
        }

        private static Mutation getMutation(String index)
        {
            Column c = new Column();
            c.name = ByteBuffer.wrap("value".getBytes());
            c.value = ByteBuffer.wrap(index.getBytes());
            c.timestamp = System.currentTimeMillis() * 1000;

            Mutation m = new Mutation();
            m.column_or_supercolumn = new ColumnOrSuperColumn();
            m.column_or_supercolumn.column = c;
            return m;
        }
    }
    
    public static class Mapper2 extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable>
    {
    	//Here is the ugly hardcoded keyword
        public String keyword = "location";
        private ByteBuffer sourceColumn;
        
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException
        {
        	sourceColumn = ByteBuffer.wrap(context.getConfiguration().get(CONF_COLUMN_NAME).getBytes());
        }

        public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
        {
            IColumn column = columns.get(sourceColumn);
            if (column == null)
                return;
            String value = ByteBufferUtil.string(column.value());
            String[] list = value.split(",");
    		for(String t : list){
    			try{
    				Integer.valueOf(t);
    				context.write(new Text(t), new IntWritable(1));
    			}catch(Exception e){
    				e.printStackTrace();
    			}	
    		}
        }
        
    }
    
    public static class Reducer2 extends Reducer<Text, IntWritable, ByteBuffer,List<Mutation>>
    {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
        	int sum = 0;
        	for(IntWritable i : values){
        		sum += i.get();
        	}
        	try{
        		context.write(ByteBuffer.wrap(key.toString().getBytes()), Collections.singletonList(getMutation(key,sum)));
        	}catch(Exception e){
        		e.printStackTrace();
        	}
        }

        private static Mutation getMutation(Text key, int sum)
        {
            Column c = new Column();
            c.name = ByteBuffer.wrap(key.toString().getBytes());
            c.value = ByteBuffer.wrap(String.valueOf(sum).getBytes());
            c.timestamp = System.currentTimeMillis() * 1000;

            Mutation m = new Mutation();
            m.column_or_supercolumn = new ColumnOrSuperColumn();
            m.column_or_supercolumn.column = c;
            return m;
        }
    }

    public int run(String[] args) throws Exception
    {

            String columnName = "value";
            getConf().set(CONF_COLUMN_NAME, columnName);
            getConf().set("mapred.job.tracker", args[0]+":8021");
            Job job = new Job(getConf(), "Phase1");
            job.setJarByClass(MapReduceCassandraDB.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setReducerClass(ReducerToCassandra.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(ByteBuffer.class);
            job.setOutputValueClass(List.class);
            
            job.setInputFormatClass(ColumnFamilyInputFormat.class);
            job.setOutputFormatClass(ColumnFamilyOutputFormat.class);
            ConfigHelper.setRangeBatchSize(job.getConfiguration(), 800);
            ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);
            
            ConfigHelper.setRpcPort(job.getConfiguration(), "9160");
            ConfigHelper.setInitialAddress(job.getConfiguration(), args[0]);
            ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
            ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
            SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBuffer.wrap(columnName.getBytes())));
            ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);


            job.waitForCompletion(true);
    	
            //Phase 2
            Job job2 = new Job(getConf(), "Phase2");
            job2.setJarByClass(MapReduceCassandraDB.class);
            job2.setMapperClass(Mapper2.class);
            job2.setReducerClass(Reducer2.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(IntWritable.class);
            job2.setOutputKeyClass(ByteBuffer.class);
            job2.setOutputValueClass(List.class);
            
            job2.setInputFormatClass(ColumnFamilyInputFormat.class);
            job2.setOutputFormatClass(ColumnFamilyOutputFormat.class);
            ConfigHelper.setOutputColumnFamily(job2.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY2);
            
            ConfigHelper.setRpcPort(job2.getConfiguration(), "9160");
            ConfigHelper.setInitialAddress(job2.getConfiguration(), args[0]);
            ConfigHelper.setPartitioner(job2.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
            ConfigHelper.setInputColumnFamily(job2.getConfiguration(), KEYSPACE,OUTPUT_COLUMN_FAMILY);
            SlicePredicate predicate2 = new SlicePredicate().setColumn_names(Arrays.asList(ByteBuffer.wrap(columnName.getBytes())));
            ConfigHelper.setInputSlicePredicate(job2.getConfiguration(), predicate2);

            job2.waitForCompletion(true);
            
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//        job.setInputFormatClass(ColumnFamilyInputFormat.class);
//        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX));
//        
//        ConfigHelper.setRpcPort(job.getConfiguration(), "9160");
//        ConfigHelper.setInitialAddress(job.getConfiguration(), args[0]);
//        ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
//        ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
//        SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBuffer.wrap(columnName.getBytes())));
//        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);
//
//        job.waitForCompletion(true);
            
            
            
        return 0;
    }
    
}
