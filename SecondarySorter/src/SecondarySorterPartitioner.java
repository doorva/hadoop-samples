import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * we need to implement our own partitioner. The reason we need to do so is that
 * now we are emitting both lastName and birthYear as the key and the defaut
 * partitioner (HashPartitioner) would then not be able to ensure that all the
 * records related to a certain lastName comes to the same reducer (partition).
 * Hence, we need to make the partitioner to only consider the actual key part
 * (lastName) while deciding on the partition for the record.
 * 
 * @author edureka
 *
 */
public class SecondarySorterPartitioner extends Partitioner<CompositeKey, Text> {

	@Override
	public int getPartition(CompositeKey key, Text value, int numReduceTasks) {
		String lastName = key.getLastName();
		return lastName.hashCode()%numReduceTasks;
	}

	/*
	 * HashPartitioner<Text, Text> hashPartitioner = new HashPartitioner<Text,
	 * Text>(); Text newKey = new Text();
	 * 
	 * @Override public int getPartition(CompositeKey key, Text value, int
	 * numReduceTasks) {
	 * 
	 * try { // Execute the default partitioner over the first part of the key
	 * newKey.set(key.getLastName()); return
	 * hashPartitioner.getPartition(newKey, value, numReduceTasks); } catch
	 * (Exception e) { e.printStackTrace(); return (int) (Math.random() *
	 * numReduceTasks); // this would return // a random value in // the range
	 * // [0,numReduceTasks) } }
	 */
}
