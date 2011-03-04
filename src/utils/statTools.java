package utils;

import java.util.ArrayList;

public class statTools {
	
	ArrayList<ArrayList<Double>> results;
	int count = 0;
	double sum = 0;
	double squareSum = 0;
	double average; 
	double standardDeviation;
	
	public statTools(ArrayList<ArrayList<Double>> results){
		for(ArrayList<Double> dArray : results){
			for(Double d : dArray){
				count += 1;
				sum += d;
			}
		}
		for(ArrayList<Double> dArray : results){
			for(Double d : dArray){
				squareSum += Math.pow((d - average),2);
			}
		}
	}
	
	public int getCount(){
		return count;
	}
	
	public double getAverage(){
		return average = sum / (double) count;
	}
	
	public double getStandardDeviation(){
		return standardDeviation = Math.sqrt(squareSum / ( (double) (count -1)));
	}

}
