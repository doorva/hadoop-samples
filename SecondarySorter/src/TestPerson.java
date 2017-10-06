
public class TestPerson {
	public static void main(String args[]){
		Person p = new Person();
		p.setFirstName("Bhrugu");
		p.setLastName("Giri");
		p.setBirthDay(10);
		p.setBirthMonth(3);
		p.setBirthYear(1980);
		System.out.println("Person is - "+p.toString());
	}
}
