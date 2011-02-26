/**
 * create keyspace Keyspace1 with replication_factor = 1 and placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy';
 * use Keyspace1;
 * create column family Standard1 with column_type = 'Standard' and comparator = 'UTF8Type';
 * create column family output_phase1 with column_type = 'Standard' and comparator = 'UTF8Type';
 * create column family output_phase2 with column_type = 'Standard' and comparator = 'UTF8Type';
 */

package implementations;

import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

import cassandra_mapreduce.MapReduceCassandraDB;

import core.BenchDB;


public class cassandraDB extends BenchDB{
	final String UTF8 = "UTF8";
	String pool = "pool";
	String keyspace = "Keyspace1";
	String colFamily = "Standard1";

	@Override
	public int connectNode(String nodeAddress) {
		int ret;
		try{
			Cluster cluster = new Cluster(nodeAddress, 9160);
			Pelops.addPool(pool, cluster, keyspace);
			ret = 1;
		}catch(Exception e){
			ret = 0;
		}
		return ret;
	}

	@Override
	public String readDB(String ID) {
		try {
			Selector selector = Pelops.createSelector(pool);
			List<Column> columns = selector.getColumnsFromRow(colFamily, ID, false, ConsistencyLevel.QUORUM);
		    return Selector.getColumnStringValue(columns, "value").toString();
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public int updateDB(String ID, String newValue) {
		int ret;
		try{
		 Mutator mutator = Pelops.createMutator(pool);
		 mutator.writeColumns(
		         colFamily, ID,
		         mutator.newColumnList(
		                 mutator.newColumn("value", newValue)
		         )
		 );
		mutator.execute(ConsistencyLevel.QUORUM);
		ret = 1;
		}catch(Exception e){
			e.printStackTrace();
			ret = 0;
		}
		return ret;
	}

	@Override
	public int writeDB(String ID, String Value) {
		return updateDB(ID, Value);
	}

	@Override
	public void searchDB(String keyword) {
		//Replace this by one your Cassandra node's IP
		String[] args = {"127.0.0.1"};
		try {
			MapReduceCassandraDB.main(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
