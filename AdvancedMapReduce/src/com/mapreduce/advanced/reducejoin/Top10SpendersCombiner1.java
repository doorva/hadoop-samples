package com.mapreduce.advanced.reducejoin;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10SpendersCombiner1 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
	int mCount = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mCount = 0;
	}
	
	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) {
			if(mCount < 5) {
				try {
					for(Text value: values) {
						context.write(key, value);
						mCount++;
					}
				} catch(Exception e) {				
				}
			}
		}
 
}
