package implementations;
/**
 * Copyright 2011 Thibault Dory
 * Licensed under the GPL Version 3 license
 */

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import org.json.JSONException;
import org.json.XML;

import core.BenchDB;
import terrastore.client.TerrastoreClient;
import terrastore.client.connection.resteasy.HTTPConnectionFactory;

/**
 * 
 * @author Thibault Dory
 * This class will not work with the current update function as it add a "1" outside of the JSON document
 */

public class terrastoreDB extends BenchDB{
	TerrastoreClient client;
	final String UTF8 = "UTF8";
	@Override
	public int connectNode(String nodeAddress) {
		int ret;
		try{
			client = new TerrastoreClient("http://"+nodeAddress+":8000", new HTTPConnectionFactory());
			ret = 1;
		}catch(Exception e){
			ret = -1;
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public String readDB(String ID) {
		String xml;
		try{
			xml = client.bucket("test").key(ID).get(String.class);
		}catch(Exception e){
			xml = null;
			e.printStackTrace();
		}
		return xml;
	}

	@Override
	public int updateDB(String ID, String newValue) {
		int ret;
		org.json.JSONObject myjson = null;
		try {
			myjson = XML.toJSONObject(newValue);
		} catch (JSONException e1) {
			e1.printStackTrace();
		}
		try{
			newValue = myjson.toString();
			CharsetDecoder utf8Decoder = Charset.forName("UTF-8").newDecoder();
			utf8Decoder.onMalformedInput(CodingErrorAction.REPLACE);
			utf8Decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
			ByteBuffer bytes = ByteBuffer.wrap(newValue.getBytes("UTF8"));
			CharBuffer parsed = utf8Decoder.decode(bytes);
			client.bucket("test").key(ID).put(parsed.toString());
			ret = 1;
		}catch(Exception e){
			ret = -1;
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public int writeDB(String ID, String Value) {
		return updateDB(ID, Value);
	}

	@Override
	public void searchDB(String keyword) {
		// TODO Auto-generated method stub
	}

	@Override
	public void close() {

	}

}
