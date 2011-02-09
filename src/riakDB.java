import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.MapReduceResponse;

/**
 * 
 * @author Thibault Dory
 *
 */

public class riakDB extends BenchDB{
	RiakClient riak;

	@Override
	public int connectNode(String nodeAddress) {
		int ret;
		try{
			riak = new RiakClient("http://"+nodeAddress+":8098/riak");
			ret = 1;
		}catch(Exception e){
			e.printStackTrace();
			ret = -1;
		}
		return ret;
	}

	@Override
	public String readDB(String ID) {
		RiakObject o;
		String ret = null;
		FetchResponse r = riak.fetch("test", ID,RequestMeta.readParams(2));
	    if (r.hasObject()){
	        o = r.getObject();
	        ret = o.getValue();
	    }
		return ret;
	}

	@Override
	public int updateDB(String ID, String newValue) {
		int ret;
		try {
			RiakObject o = new RiakObject(riak,"test",ID,newValue.getBytes("UTF8"));
			o.store(RequestMeta.writeParams(2, 2));
			ret = 1;
		} catch(Exception e){
			e.printStackTrace();
			ret = -1;
		}
		return ret;
	}

	@Override
	public int writeDB(String ID, String Value) {
		return updateDB(ID, Value);
	}
	
	public void searchDB(String keyword){

		String job = "{\"inputs\":\"test\",\"query\":[{\"map\":{\"language\":\"javascript\",\"source\":\"" +
				"function(v, keyData, arg) { var words = v.values[0].data.split(' '); var keyword = arg; " +
				"var results = []; for ( var i=0; i<words.length; i++ ) { if(words[i].toLowerCase() == keyword.toLowerCase() ){ " +
				"temp = {}; temp[words[i]] = [v.key]; results.push(temp); } } return results; }\",\"arg\":\""+keyword+"\"}}" +
				",{\"reduce\":{\"language\":\"javascript\",\"source\":\"" +
				"function(v) { var r = {}; for (var i in v) { for(var w in v[i]) { if (w in r) r[w] = r[w].concat(v[i][w]); " +
				"else r[w] = v[i][w]; } } ret = []; for (var key in r){ temp ={}; temp[key] = r[key]; ret.push(temp); } return ret; }\"" +
				"}},{\"reduce\":{\"language\":\"javascript\",\"source\":\"function(v) { var r = []; for(var i in v){ " +
				"for(var w in v[i]){ for(var id in v[i][w]){ temp = {}; temp[v[i][w][id]] = 1; r.push(temp); } } } return r; }\"}}" +
				",{\"reduce\":{\"language\":\"javascript\",\"source\":\"function(v) { r = {} ; for(var i in v){ " +
				"for(var w in v[i]){ if(w in r) r[w] += v[i][w]; else r[w] = v[i][w]; } } return [r]; }\"}}],\"timeout\": 12000000}";
		
		MapReduceResponse tempRes = riak.mapReduce(job);

		System.out.println(tempRes.getBodyAsString());
	}
	

}
















