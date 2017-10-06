import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class SecondarySorterDriver extends Configured {
	private static final Logger logger = Logger.getLogger(SecondarySorterDriver.class);
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		logger.info("Beginning execution of the secondary sorter example");
		
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[1]), true);
		
		// TODO Auto-generated method stub
		Job job = Job.getInstance();
		job.setJarByClass(SecondarySorterDriver.class);
		job.setNumReduceTasks(2);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(SecondarySorterMapper.class);
		job.setReducerClass(SecondarySorterReducer.class); 

		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(SecondarySorterPartitioner.class);
		job.setGroupingComparatorClass(SecondarySorterGroupingComparator.class);
		job.setSortComparatorClass(SecondarySorterComparator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		logger.info("Secondary sorting process complete.");
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
