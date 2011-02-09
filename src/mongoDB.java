import java.net.UnknownHostException;
import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceOutput;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

/**
 * 
 * @author Thibault Dory
 *
 */

public class mongoDB extends BenchDB{
	DB db;
	Mongo mdb;
	DBCollection dbCol;
	 
	@Override
	public int connectNode(String nodeAddress) {
		int ret;
		try {
			mdb = new Mongo(nodeAddress);
			db = mdb.getDB("test");
			dbCol = db.getCollection("test");
			DBCursor cur = dbCol.find();
			ret = 1;
		} catch (UnknownHostException e) {
			ret = -1;
			e.printStackTrace();
		} catch (MongoException e) {
			ret = 1;
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public String readDB(String ID) {
		String ret;
		try{
			db.requestStart();
			BasicDBObject query = new BasicDBObject();
			query.put("_id", ID);
			DBCursor cur = dbCol.find(query);
			ret = "";
			while(cur.hasNext()) {
				DBObject temp = (DBObject) JSON.parse(cur.next().toString());
	            ret = (String) temp.get("artValue");
	           
	        }
			db.requestDone();
		}catch(Exception e){
			e.printStackTrace();
			ret = null;
		}
		return ret;
	}


	public int writeDB(String ID, String newValue) {
		int ret;
		try{
			db.requestStart();
			BasicDBObject doc = new BasicDBObject();
			doc.put("_id",ID);
			doc.put("artValue", newValue);
			dbCol.insert(doc);
			db.requestDone();
			ret = 1;
		}catch(Exception e){
			e.printStackTrace();
			ret = -1;
		}
		return ret;
	}

	@Override
	public int updateDB(String ID, String newValue) {
		int ret;
		try{
			db.requestStart();
			BasicDBObject query = new BasicDBObject();
			query.put("_id", ID);
			DBCursor cur = dbCol.find(query);
			cur.next();
			BasicDBObject doc = new BasicDBObject();
			doc.put("_id",ID);
			doc.put("artValue", newValue);
			dbCol.update(cur.curr(), doc);
			db.requestDone();
			ret = 1;
		}catch(Exception e){
			e.printStackTrace();
			ret = -1;
		}
		return ret;
	}
	
	public void searchDB(String keyword){
		ArrayList<String> res= new ArrayList<String>();
		String map_index = 	"function() {"+
							"var words = this.artValue.split(' ');"+
							"var keyword = \""+keyword+"\";"+
							"for ( var i=0; i<words.length; i++ ) {"+
								"if(words[i].toLowerCase()  == keyword.toLowerCase() ){"+
									"emit(words[i], { docs: [this._id] });"+
      							"}}}";
		
		String reduce_index = 	"function(key, values) {"+
								"var docs = [];"+
								"values.forEach ( function(val) { docs = docs.concat(val.docs); });"+
								"return { docs: docs };}";
		
		
		String map_relevance = 	"function() {"+
								"for ( var i=0; i< this.value.docs.length; i++ ) {"+
								"emit(this.value.docs[i], { count: 1 });}}";
		
		String reduce_relevance = 	"function(key, values) {"+
									"var sum = 0;"+
									"values.forEach ( function(val) { sum += val.count; });"+
									"return { count: sum };}";

		//First MapReduce phase
		MapReduceOutput tempRes = dbCol.mapReduce(map_index, reduce_index, null, null);
		DBCollection outCol = tempRes.getOutputCollection();
		//Second MapReduce phase
		MapReduceOutput tempRes2 = outCol.mapReduce(map_relevance, reduce_relevance, null, null);
		DBCollection outCol2 = tempRes2.getOutputCollection();
		DBCursor cur = outCol2.find();
		while(cur.hasNext()){
			res.add(cur.next().toString());
		}
		System.out.println(res);
	}

}
















