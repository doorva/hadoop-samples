import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This class is the actual value object that represents
 * the data coming in the file.
 * 
 * @author edureka
 *
 */
public class Person implements Writable{

	private String firstName;
	private String lastName;
	private int birthYear;
	private int birthMonth;
	private int birthDay;
	
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
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
	public int getBirthMonth() {
		return birthMonth;
	}
	public void setBirthMonth(int birthMonth) {
		this.birthMonth = birthMonth;
	}
	public int getBirthDay() {
		return birthDay;
	}
	public void setBirthDay(int birthDay) {
		this.birthDay = birthDay;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + birthDay;
		result = prime * result + birthMonth;
		result = prime * result + birthYear;
		result = prime * result + ((firstName == null) ? 0 : firstName.hashCode());
		result = prime * result + ((lastName == null) ? 0 : lastName.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Person other = (Person) obj;
		if (birthDay != other.birthDay)
			return false;
		if (birthMonth != other.birthMonth)
			return false;
		if (birthYear != other.birthYear)
			return false;
		if (firstName == null) {
			if (other.firstName != null)
				return false;
		} else if (!firstName.equals(other.firstName))
			return false;
		if (lastName == null) {
			if (other.lastName != null)
				return false;
		} else if (!lastName.equals(other.lastName))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "Person [firstName=" + firstName + ", lastName=" + lastName + ", birthYear=" + birthYear
				+ ", birthMonth=" + birthMonth + ", birthDay=" + birthDay + "]";
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		lastName = WritableUtils.readString(in);
		firstName = WritableUtils.readString(in);
		birthYear = Integer.parseInt(WritableUtils.readString(in));
		birthMonth = Integer.parseInt(WritableUtils.readString(in));
		birthDay = Integer.parseInt(WritableUtils.readString(in));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, lastName);
		WritableUtils.writeString(out, firstName);
		WritableUtils.writeString(out, String.valueOf(birthYear));
		WritableUtils.writeString(out, String.valueOf(birthMonth));
		WritableUtils.writeString(out, String.valueOf(birthDay));
	}

	
		
	
}
