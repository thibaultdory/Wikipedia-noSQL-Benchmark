package utils;

import java.util.ArrayList;

public class argsTools {
	
	public static ArrayList<ArrayList<String>> divideNodeList(ArrayList<String> nodeList, int numberOfClients){
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		int numberOfNodes = nodeList.size();
		int low=0;
		int up=numberOfNodes/numberOfClients;
		
		for(int i=0;i<numberOfClients;i++){
			ArrayList<String> local = new ArrayList<String>();
			for(int j=low;j<up;j++){
				if(j<nodeList.size())
					local.add(nodeList.get(j));
				if(j==(nodeList.size()-(numberOfNodes%numberOfClients)-1)){
					for(int u=1;u<=(numberOfNodes%numberOfClients);u++){
						local.add(nodeList.get(j+u));
					}
				}
			}
			result.add(local);
			low = up;
			up += numberOfNodes/numberOfClients;
		}
		return result;
	}

}
