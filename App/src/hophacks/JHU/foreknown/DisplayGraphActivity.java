package hophacks.JHU.foreknown;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.StringTokenizer;

import android.content.res.AssetManager;
import android.os.Bundle;
import android.support.v7.app.ActionBarActivity;
import android.widget.Button;
import android.widget.LinearLayout;

public class DisplayGraphActivity extends ActionBarActivity {
	

	/** Called when the activity is first created. */
    @Override
    public void onCreate(Bundle savedInstanceState) {
    	
    	double[] prevData = null;
    	try{
    		prevData = readAndInsert("2013r.csv");
    	} catch (UnsupportedEncodingException e) {
    		e.printStackTrace();
    		System.out.println("FLIP A SHIT AND RUN OUT OF THE ROOM");
    	}
    	
    	double[] newDataDouble = null;
    	float[] newData = null;
    	
    	if (prevData == null) {
    		System.out.println("WE GOTS NO DATA CAP'N");
    		throw new NullPointerException();
    	} else {
    		newDataDouble = PredictionMath.predict(prevData);
    		newData = new float[newDataDouble.length];
    		for(int j = 0; j < newDataDouble.length;j++) {
    			newData[j] = (float) newDataDouble[j];
    		}
    	}
    	
    	float[] totalData = new float[prevData.length + newData.length];
    	
    	for(int q = 0; q < prevData.length; q++) {
    		totalData[q] = (float) prevData[q];
    	}
    	
    	for(int l = 0; l < newData.length; l++) {
    		totalData[prevData.length + l] = newData[l];
    	}
    	
    	super.onCreate(savedInstanceState);
        
        LinearLayout buttonGraph = new LinearLayout(this);
        buttonGraph.setOrientation(LinearLayout.VERTICAL);
        
        LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(LinearLayout.LayoutParams.WRAP_CONTENT, LinearLayout.LayoutParams.WRAP_CONTENT); // Verbose!
        
        float[] days = new float[totalData.length];
        for(int k = 0; k < totalData.length; k++) {
        	days[k] = Float.parseFloat(Integer.toString(k));
        }
        
        float[] xvalues = days;
        float[] yvalues = totalData;

        int color = prevData.length;
        
        plot2d graph = new plot2d(this, xvalues, yvalues, 1, color);
        
        buttonGraph.addView(graph, lp);

        setContentView(buttonGraph);
        
    }
    
    private double[] readAndInsert(String csvSource) throws UnsupportedEncodingException {

    	ArrayList<String> objList= new ArrayList<String>();
    	AssetManager assetManager = getAssets();
    	InputStream is = null;

    	try {
            is = assetManager.open(csvSource);
        } catch (IOException e) {
            e.printStackTrace();
        }

        BufferedReader reader = null;
        reader = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));

        String line = "";
        StringTokenizer st = null;
        try {

            while ((line = reader.readLine()) != null) {
                st = new StringTokenizer(line, ",");
                String obj= new String ();
                                //your attributes
                obj = st.nextToken();

                objList.add(obj);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        double[] data = new double[objList.size()];
        
        for(int i = 0; i < objList.size(); i++) {
        	data[i] = Double.parseDouble(objList.get(i));
        	System.out.println(objList.get(i));
        }
        
        return data;
	}

}
