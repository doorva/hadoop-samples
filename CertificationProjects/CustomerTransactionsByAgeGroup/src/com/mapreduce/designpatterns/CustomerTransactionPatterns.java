package com.mapreduce.designpatterns;

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
import org.apache.log4j.Logger;

/**
 * Problem Statement : Given 2 data sets Customer & Transactions find Top 5
 * Spenders with Name,TotalSpend in each of the given Age Groups(
 * <30,30-50,50-70,>70) This Program demonstrates the use of following design
 * patterns: 1)Filter Pattern 2)Join Pattern 3)Aggregation Pattern(count,Sum)
 * 4)Sorting Pattern(Decreasing Comparator) 5)Top N values Pattern
 * 
 * @author edureka
 *
 */
public class CustomerTransactionPatterns {
	private static final Logger LOGGER = Logger.getLogger(CustomerTransactionPatterns.class);
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Job job = Job.getInstance();
		job.setJarByClass(CustomerTransactionPatterns.class);
		job.setNumReduceTasks(1);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
		
		job.setReducerClass(CustomerTransactionReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[2]), true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	static class CustomerMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String str = value.toString();
			String[] strarr = str.split(",");
			context.write(new Text(strarr[0]), new Text("cust\t" + strarr[1] + "\t" + strarr[2]+ "\t" + strarr[3]));
		}
	}

	static class TransactionMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			String[] strarr = str.split(",");
			context.write(new Text(strarr[2]), new Text("txns\t" + strarr[3]));
		}

	}

	static class CustomerTransactionReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String fullname = "";
			String age = "";
			int count = 0;
			double totalSpent = 0.0;
			for (Text t : values) {
				String s = t.toString();
				String[] arr = s.split("\t");
				if (arr[0].equals("txns")) {
					totalSpent += Double.parseDouble(arr[1]);
					count++;
				} else if (arr[0].equals("cust")) {
					fullname = arr[1] + " " + arr[2];
					age = arr[3];
				}
			}
			String result = String.format("%d\t%f\t%s", count, totalSpent, age);
			context.write(new Text(fullname), new Text(result));

		}

	}
}
