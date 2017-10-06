import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class SecondarySorterMapper extends Mapper<LongWritable,Text,CompositeKey,Text>{
	private static final Logger logger = Logger.getLogger(SecondarySorterMapper.class);
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String data = value.toString();
		
		logger.info("The value string from file is :" + value);
		
		String[] fields = data.split(",");
		
		logger.info("Total number of attributes from value into fields: " + fields.length);
		
		//Person p = createPerson(fields);
		CompositeKey compositeKey = new CompositeKey(fields[0], Integer.parseInt(fields[2]));
		
		logger.info("The Composite key is : "+ compositeKey.toString());
		
		context.write(compositeKey, value);
		//context.write(new Text(fields[0]), value);

	}
	
	/*private Person createPerson(String[] fields){
		Person person = new Person();
		person.setLastName(fields[0]);
		person.setFirstName(fields[1]);
		person.setBirthYear(Integer.parseInt(fields[2]));
		person.setBirthMonth(Integer.parseInt(fields[3]));
		person.setBirthDay(Integer.parseInt(fields[4]));
		return person;
	}
*/
}
