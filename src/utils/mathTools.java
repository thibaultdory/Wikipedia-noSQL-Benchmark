package utils;

import java.util.ArrayList;


public class mathTools {
	
	public static double numIntegral(ArrayList<Double> average, ArrayList<Double> time){
		double FullIntegral =0;
		
		for(int i=0;i<average.size()-1;i++){
			double rectangle = (time.get(i+1) - time.get(i))*Math.max(average.get(i), average.get(i+1));
			double triangleUp = Math.abs(average.get(i)-average.get(i+1))*(time.get(i+1) - time.get(i))/2;
			FullIntegral += rectangle - triangleUp;
		}
		
		return FullIntegral;
	}
	
	public static double elasticityScalar(ArrayList<Double> average, ArrayList<Double> time, ArrayList<Double> sd, int M, int N){
		double under = (time.get(time.size()-1) - time.get(0)) * average.get(average.size()-1);
		double A = numIntegral(average, time);
		ArrayList<Double> averageSD = new ArrayList<Double>();
		for(int i=0;i<average.size();i++){
			averageSD.add(average.get(i)+sd.get(i));
		}
		
		double B = numIntegral(averageSD, time);
		
		double res = (A + B - 2 * under)/(N + M);
		
		return res;
	}
}
