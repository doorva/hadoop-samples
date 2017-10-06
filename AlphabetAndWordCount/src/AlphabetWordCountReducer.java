import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AlphabetWordCountReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
	
	public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
		
		int count = 0;
		while(values.iterator().hasNext()){
			IntWritable i = values.iterator().next();
			count = count + i.get();
		}
		context.write(key, new IntWritable(count));
	}

}
