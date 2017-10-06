import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Now, the partitioner only makes sure that the all records related to the same last name
 * comes to a particular reducer, but it doesn't guarantee that all of them will come 
 * in the same input group (i.e. in a single reduce() call as the list of values). 
 * In order to make sure of this, we will need to implement our own grouping comparator. 
 * We shall do similar thing as we did for the partitioner, i.e. only look at the actual 
 * key (lastName) for grouping of reducer inputs.
 * 
 * @author edureka
 *
 */
public class SecondarySorterGroupingComparator extends WritableComparator {

	protected SecondarySorterGroupingComparator() {
		super(CompositeKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;

		// (check on udid)
		return key1.getLastName().compareTo(key2.getLastName());
	}
}