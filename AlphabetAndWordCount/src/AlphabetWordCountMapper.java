import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AlphabetWordCountMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		
		String s= value.toString();
		String[] strarr = s.split(" ");
		for(String word:strarr){
			if(word.length()>0){
				context.write(new IntWritable(word.length()), new IntWritable(1));
			}
		}
	}

}
