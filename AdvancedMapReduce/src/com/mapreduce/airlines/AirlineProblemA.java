package com.mapreduce.airlines;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AirlineProblemA {

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Job job = Job.getInstance();
		job.setJarByClass(AirlineProblemA.class);
		job.setMapperClass(ProblemAMapper.class);
		job.setNumReduceTasks(0);

		// add the input file
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// add the output path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// set the output key and value types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// delete the output path if it already exists
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[1]), true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	static class ProblemAMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String str = value.toString();
			String[] arr = str.split(",");
			String country = arr[3];
			String airportname = arr[1];
			if (country.equalsIgnoreCase("India")){
				context.write(new Text(country), new Text(airportname));
			}

		}

	}
}
