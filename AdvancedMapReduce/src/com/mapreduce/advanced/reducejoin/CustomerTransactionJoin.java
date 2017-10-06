package com.mapreduce.advanced.reducejoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomerTransactionJoin {

	static class CustomerTransactionJoinCustomerMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			String[] strarr = str.split(",");
			context.write(new Text(strarr[0]), new Text("cust\t" + strarr[1]));
		}
	}

	static class CustomerTransactionJoinTransactionMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			String[] strarr = str.split(",");
			context.write(new Text(strarr[2]), new Text("txns\t" + strarr[3]));
		}

	}

	static class CustomerTransactionJoinReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

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

	/**
	 * Usage:
	 * hadoop jar CustomerTransactionJoin.jar /MyProjects/custs /MyProjects/txns /MyProjects/CustomerTransactionJoinOutput
	 * 
	 * 
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Job job = Job.getInstance();
		job.setJarByClass(CustomerTransactionJoin.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerTransactionJoinCustomerMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CustomerTransactionJoinTransactionMapper.class);

		job.setReducerClass(CustomerTransactionJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[2]), true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
