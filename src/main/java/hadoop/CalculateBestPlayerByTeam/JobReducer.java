package hadoop.CalculateBestPlayerByTeam;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JobReducer extends Reducer<Text, MapWritable, Text, Text>{

	
	HashMap<Text, Integer> hm = new HashMap<Text, Integer>();
	@Override
	protected void reduce(Text key, Iterable<MapWritable> values,
			Context context)
			throws IOException, InterruptedException {
	
		
				Iterator<MapWritable> valuesIt = values.iterator();
		
		while(valuesIt.hasNext()){
			MapWritable mw = valuesIt.next();
			Text name = (Text)mw.get("name");
			IntWritable po = (IntWritable) mw.get("points");
			System.out.println(new String(name.getBytes()));
			//addPlayer(new String(name.getBytes(),StandardCharsets.UTF_8),po.get());
			addPlayer(name,po.get());
		}
		
		int totPoints = 0;
		String playerName="";
		
		 Set set = hm.entrySet();
	      // Get an iterator
	      Iterator i = set.iterator();
	      // Display elements
	      while(i.hasNext()) {
	         Map.Entry me = (Map.Entry)i.next();
	         
	         if((Integer)me.getValue()>totPoints){
	        	 totPoints = (Integer)me.getValue();
	        	 playerName=(String)me.getKey();
	        	 
	         }
	      }
		context.write(key, new Text(playerName));
	}	
	
	public void addPlayer(Text name, int points){
		if(hm.containsKey(name)){
			hm.put(name, hm.get(name)+points);
		}else{
			hm.put(name, points);
		}
		
		
	}
}