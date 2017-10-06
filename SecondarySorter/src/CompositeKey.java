import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This class defines the composite key for the 
 * Person Data set.
 * 
 * @author edureka
 * @see Person.java
 */
public class CompositeKey implements WritableComparable<CompositeKey> {
	private String lastName;
	private int birthYear;

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public int getBirthYear() {
		return birthYear;
	}

	public void setBirthYear(int birthYear) {
		this.birthYear = birthYear;
	}

	public CompositeKey() {
	}

	public CompositeKey(String lastName, int birthYear) {
		this.lastName = lastName;
		this.birthYear = birthYear;
	}

//	@Override
//	public String toString() {
//
//		return (new StringBuilder()).append(lastName).append(',').append(birthYear).toString();
//	}

	@Override
	public void readFields(DataInput in) throws IOException {

		lastName = WritableUtils.readString(in);
		birthYear = Integer.parseInt(WritableUtils.readString(in));
	}

	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, lastName);
		WritableUtils.writeString(out, String.valueOf(birthYear));
	}

	@Override
	public int compareTo(CompositeKey o) {

		int result = lastName.compareTo(o.lastName);
		if (0 == result) {
			result = (birthYear-o.birthYear)==0?0:(birthYear-o.birthYear>0)?1:-1;
		}
		return result;
	}
}
