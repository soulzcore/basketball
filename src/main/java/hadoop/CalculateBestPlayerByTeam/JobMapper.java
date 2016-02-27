package hadoop.CalculateBestPlayerByTeam;

import java.io.IOException;
import java.util.StringTokenizer;
 


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class JobMapper extends Mapper<LongWritable, Text, Text, MapWritable>{
 
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] records = line.split(",");
		String points = records[8].replace("\"", "");;
		MapWritable mp = new MapWritable();
		
		
		mp.put(new Text("name"),new Text(records[0]));
		mp.put(new Text("points"),new Text(records[8]));
	
		
		IntWritable p = new IntWritable(Integer.parseInt(points));
		StringTokenizer st = new StringTokenizer(line," ");
		context.write(new Text(records[3]),mp );
		
		/*
		while(st.hasMoreTokens()){
			word.set(st.nextToken());
			context.write(word,one);
		}
		*/
	}
}