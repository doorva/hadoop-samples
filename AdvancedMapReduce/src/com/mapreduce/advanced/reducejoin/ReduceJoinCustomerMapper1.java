package com.mapreduce.advanced.reducejoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ReduceJoinCustomerMapper1 extends Mapper<LongWritable,Text,Text,Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String str = value.toString();
		String[] strarr = str.split(",");
		context.write(new Text(strarr[0]), new Text("cust\t"+strarr[1]));
	}
	

}
