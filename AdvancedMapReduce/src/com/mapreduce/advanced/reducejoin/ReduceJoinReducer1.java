package com.mapreduce.advanced.reducejoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceJoinReducer1 extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String name = "";
		int count = 0;
		double totalSpent = 0.0;
		for (Text t : values) {
			String s = t.toString();
			String[] arr = s.split("\t");
			if (arr[0].equals("txns")) {
				totalSpent += Double.parseDouble(arr[1]);
				count++;
			} else if (arr[0].equals("cust")) {
				name = arr[1];
			}
		}
		String result = String.format("%d\t%f", count, totalSpent);
		context.write(new Text(name), new Text(result));

	}

}
