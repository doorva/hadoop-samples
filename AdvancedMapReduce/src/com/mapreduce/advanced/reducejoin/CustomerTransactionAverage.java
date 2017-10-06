package com.mapreduce.advanced.reducejoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable.Comparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CustomerTransactionAverage {
	static class CustomerTransactionAverageTransactionMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			String[] strarr = str.split(",");
			context.write(new Text(strarr[4]), new Text(strarr[3]));
		}

	}

	static class CustomerTransactionAverageReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double totalSpent = 0.0;
			int count=0;
			for (Text t : values) {
				totalSpent = totalSpent + Double.parseDouble(t.toString());
				count++;
			}
			
			double average = totalSpent/count;
			String result = String.format("%f",average);
			context.write(new Text(key.toString()), new Text(result));
		}
	}

	static class DecreasingDoubleWritableComparator extends Comparator {

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// TODO Auto-generated method stub
			return -1 * super.compare(b1, s1, l1, b2, s2, l2);
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

		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setMapperClass(CustomerTransactionAverageTransactionMapper.class);
		
		job.setReducerClass(CustomerTransactionAverageReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setSortComparatorClass(DecreasingDoubleWritableComparator.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[1]), true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
