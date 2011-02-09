/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */

package cassandra_mapred;


import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.SortedMap;
import org.apache.log4j.Logger;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

    public static void main(String[] args) throws Exception
    {
    	//Delete old results if any
    	Files.deleteDirectory(new File(OUTPUT_PATH_PREFIX));
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new MapReduceCassandraDB(), args);
    }

    public static class TokenizerMapper extends Mapper<String, SortedMap<byte[], IColumn>, Text, Text>
    {
    	//Here is the ugly hardcoded keyword
        public String keyword = "location";
        private String columnName;

        public void map(String key, SortedMap<byte[], IColumn> columns, Context context) throws IOException, InterruptedException
        {
            IColumn column = columns.get(columnName.getBytes());
            if (column == null)
                return;
            String value = new String(column.value());
            String [] words = value.split(" ");
            Text ID = new Text(key);
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

        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException
        {
            this.columnName = context.getConfiguration().get(CONF_COLUMN_NAME);
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

    public int run(String[] args) throws Exception
    {

            String columnName = "value";
            getConf().set(CONF_COLUMN_NAME, columnName);
            Job job = new Job(getConf(), "Phase1");
            job.setJarByClass(MapReduceCassandraDB.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(ColumnFamilyInputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX));

            ConfigHelper.setThriftContact(job.getConfiguration(), args[0],  9160);
            ConfigHelper.setColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
            SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(columnName.getBytes()));
            ConfigHelper.setSlicePredicate(job.getConfiguration(), predicate);

            job.waitForCompletion(true);
            
            
            
        return 0;
    }
    
}
