package hophacks.JHU.foreknown;

import android.os.Bundle;
import android.support.v7.app.ActionBarActivity;
import android.widget.Button;
import android.widget.LinearLayout;

public class DisplayGraphActivity extends ActionBarActivity {

	/** Called when the activity is first created. */
    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        
        LinearLayout buttonGraph = new LinearLayout(this);
        buttonGraph.setOrientation(LinearLayout.VERTICAL);
        
        
        LinearLayout buttons = new LinearLayout(this);
        buttons.setOrientation(LinearLayout.HORIZONTAL);
        Button Start = new Button(this);
        Start.setText("Test Button 1");
        Button Stop = new Button(this);
        Stop.setText("Test Button 2");
        LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(LinearLayout.LayoutParams.WRAP_CONTENT, LinearLayout.LayoutParams.WRAP_CONTENT); // Verbose!
        
        buttons.addView(Start, lp);
        buttons.addView(Stop, lp);
        buttonGraph.addView(buttons, lp);
       
        float[] xvalues = new float[] { -1.0f, 1.0f, 2.0f, 3.0f , 4.0f, 5.0f, 6.0f };
        float[] yvalues = new float[] { 15.0f, 2.0f, 0.0f, 2.0f, -2.5f, -1.0f , -3.0f };
        
        /*float[] xvalues = new float[1201];
        float[] yvalues = new float[1201];
        for (int i=0;i<1201;i++){
        	double temp = (-5+i*.01);
        	xvalues[i] = (float)temp;
        	yvalues[i] = (float)(Math.sin(temp)*Math.random());
        }*/
        
        plot2d graph = new plot2d(this, xvalues, yvalues, 1);

        buttonGraph.addView(graph, lp);

        setContentView(buttonGraph);
        
    }
}
