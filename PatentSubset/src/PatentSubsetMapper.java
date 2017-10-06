import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PatentSubsetMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		
		String s= value.toString();
		String[] strarr = s.split(" ");
		context.write(new Text(strarr[0]), new Text(strarr[1]));
	}

}
