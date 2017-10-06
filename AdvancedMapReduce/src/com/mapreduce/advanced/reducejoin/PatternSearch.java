/**
 * 
 */
package com.mapreduce.advanced.reducejoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author edureka
 *
 */
public class PatternSearch {

	
	public static void main(String args[]) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		conf.set("pattern", args[2]);
		//Job job = new Job(conf);		
		Job job = Job.getInstance(conf);
		
		job.setJobName("Find Word Occurence");
		job.setJarByClass(PatternSearch.class);
		
		job.setMapperClass(PatternSearchMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	static class PatternSearchMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		String mSearchPattern = null;
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String strArr[] = value.toString().split("\t");
			if(strArr[1].contains(mSearchPattern)) {
				context.write(new Text(strArr[0]), new Text(strArr[1]));
			}
		}
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			mSearchPattern = conf.get("pattern");
		}	
	}
}