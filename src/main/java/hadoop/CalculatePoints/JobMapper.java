package hadoop.CalculatePoints;

import java.io.IOException;
import java.util.StringTokenizer;
 

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class JobMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
 
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] records = line.split(",");
		String points = records[8];
		IntWritable p = new IntWritable(Integer.parseInt(points));
		StringTokenizer st = new StringTokenizer(line," ");
		context.write(new Text(records[0]),p );
		
		/*
		while(st.hasMoreTokens()){
			word.set(st.nextToken());
			context.write(word,one);
		}
		*/
	}
}