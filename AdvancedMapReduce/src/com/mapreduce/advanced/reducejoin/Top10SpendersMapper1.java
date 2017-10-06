package com.mapreduce.advanced.reducejoin;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10SpendersMapper1 extends Mapper<Text, Text, DoubleWritable, Text> {
	public void map(Text key, Text value, Context context) {
		try {
			String[] values = value.toString().split("\t");
			Double totalSpend = Double.parseDouble(values[1]);
			context.write(new DoubleWritable(totalSpend), key);
		} catch (Exception e){
			System.out.println(e.getMessage());			
		}
	}
}
