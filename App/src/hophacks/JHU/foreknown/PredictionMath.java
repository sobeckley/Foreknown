package hophacks.JHU.foreknown;

import java.util.*;
import java.lang.Math;

public class PredictionMath {
//	public static void main(String[] args) {
//		double[] sampleData = new double[] {58.7, 56.85, 57, 59.6, 60};
//
//		double[] data = fancyMath(sampleData);
//		for (int i = 0; i < data.length;i++) {
//			System.out.println(data[i]+"\n");
//		}
//	}

	public static double[] predict(double[] arr) {
	
	
		// Declare an array that will hold the S Ratios 
	
		double[] lnSRatios = new double[arr.length - 1];
		for(int i = 0; i<arr.length - 1;i++){
			lnSRatios[i] = Math.log(arr[i+1]/arr[i]);
		}
	

		double sumRatios = 0;	
		for(int i = 0; i<lnSRatios.length;i++){
			sumRatios += lnSRatios[i]; 
		}
		
		// Calculate U and S^2 which will be used to compute drift and volatility
		double uBar = (1.0/lnSRatios.length)*sumRatios;
		double sSquared = (1.0/(lnSRatios.length - 1))*(Math.pow(sumRatios - uBar, 2));
		
		
		//Stock drift and volatility
		
		double dt = 1;
		double tFinal = 252;
		double drift = Math.sqrt(sSquared)/Math.sqrt(dt);
		double volatility = (uBar + sSquared/2)/dt;
		double So = arr[arr.length - 1];
		double t = 0.0;
		
		Random r = new Random();
		double Wt = 0;
		double[] St = new double[(int)(tFinal/dt)];
		int counter = 0;
		
		
		
		while (counter < St.length  &&  t<= tFinal){
			Wt = 1.618*So*(r.nextGaussian()*Math.sqrt(dt));
			St[counter] = So + (drift - (Math.pow(volatility, 2))/2)*dt + volatility*Wt*Math.sqrt(dt);
			So = St[counter];
			counter ++;
			t += dt;
		}
		
		return St;
	}	
	
}