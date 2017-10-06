package com.mapreduce.designpatterns;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.DoubleWritable.Comparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.log4j.Logger;


public class TopNSpendersByAgeGroup {
	//private static final Logger LOGGER = Logger.getLogger(TopNSpendersByAgeGroup.class);

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException  {
		Configuration conf = new Configuration();
		conf.setInt("topN", Integer.parseInt(args[2]));
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(TopNSpendersByAgeGroup.class);
		job.setNumReduceTasks(1);
		job.setJobName("Top N highest spending customers By Age Group");
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		
		job.setMapperClass(TopNSpendersMapper.class);
		job.setCombinerClass(TopNSpendersCombiner.class);
		job.setSortComparatorClass(DecreasingDoubleWritableComparator.class);
		job.setReducerClass(TopNSpendersReducer.class);
		
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[1]), true);
		 
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
	
	static class TopNSpendersMapper extends Mapper<Text, Text, DoubleWritable, Text> {
		public void map(Text key, Text value, Context context) {
			try {
				Double totalSpend = Double.parseDouble(value.toString());
				context.write(new DoubleWritable(totalSpend), key);
			} catch (Exception e){
				System.out.println(e.getMessage());			
			}
		}
	}
 
	static class TopNSpendersCombiner extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
		int mCount = 0;
		int topN = 0;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			mCount = 0;
			topN = context.getConfiguration().getInt("topN", 0);
		}
		
		@Override
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) {
				if(mCount < topN) {
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
	
	static class TopNSpendersReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		int mCount = 0;
		int topN = 0;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			mCount = 0;
			topN = context.getConfiguration().getInt("topN", 0);
		}
		
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) {
			if(mCount < topN) {
				try {
					for(Text value: values) {
						context.write(value, key);
						mCount++;
					}
				} catch(Exception e) {
					
				}
			}
		}
	}
	
	static class DecreasingDoubleWritableComparator extends Comparator {

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// TODO Auto-generated method stub
			return -1 * super.compare(b1, s1, l1, b2, s2, l2);
		}
	}
	
}
