package hophacks.JHU.foreknown;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.Number;
 
public class ReadCSV {
 
  public float[] run(String file) {
 
	//String csvFile = file;
	BufferedReader br = null;
	String line = "";
	String cvsSplitBy = ",";
	String[] data = new String[252];
	
	try {
 
		//br = new BufferedReader(new FileReader(getAssets().open(file)));
		while ((line = br.readLine()) != null) {
 
		    // use comma as separator
			data = line.split(cvsSplitBy);
 
			for(int i = 0; i < data.length; i++) {
				
				if(data[i]==null) {
					break;	
				}
				
				System.out.println(data[i]);
			}
 
		}
 
	} catch (FileNotFoundException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	} finally {
		if (br != null) {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
 
	float[] dataFloat = new float[252];
	for(int i = 0; i < data.length; i++) {
		
		if(data[i]==null) {
			break;	
		}
		
		dataFloat[i] = Float.parseFloat(data[i]);
	}
	
	System.out.println("Done");
	return dataFloat;
  }
 
}