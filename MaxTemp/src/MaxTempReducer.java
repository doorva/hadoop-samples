import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
	
	public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
		
		int max = 0;
		while(values.iterator().hasNext()){
			IntWritable i = values.iterator().next();
			if(i.get()>=max){
				max=i.get();
			}
			//count = count + i.get();
		}
		context.write(key, new IntWritable(max));
	}

}
