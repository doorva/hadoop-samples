package com.mapreduce.advanced.reducejoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReduceJoinTransactionMapper1 extends Mapper<LongWritable,Text,Text,Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String str = value.toString();
		String[] strarr = str.split(",");
		context.write(new Text(strarr[2]), new Text("txns\t"+strarr[3]));
	}
	

}

