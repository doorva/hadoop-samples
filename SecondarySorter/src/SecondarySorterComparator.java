import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * The comparator to compare two objects from one another
 * 
 * @author edureka
 *
 */
public class SecondarySorterComparator extends WritableComparator {
	protected SecondarySorterComparator() {
		super(CompositeKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;

		// (first check on last name)
		int compare = key1.getLastName().compareTo(key2.getLastName());

		if (compare == 0) {
			// only if we are in the same input group should we try and sort by
			// value (datetime)
			return (key1.getBirthYear() - key2.getBirthYear()) == 0 ? 0
					: (key1.getBirthYear() - key2.getBirthYear() > 0) ? 1 : -1;
		}

		return compare;
	}
}