import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySorterReducer extends Reducer<CompositeKey, Text, Text, Text> {

	@Override
	protected void reduce(CompositeKey key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		while(values.iterator().hasNext()){
			context.write(new Text(key.getLastName()), values.iterator().next());
		}
	}

	/*@Override
	protected void reduce(CompositeKey arg0, Iterable<Person> arg1,
			Reducer<CompositeKey, Person, Text, Person>.Context context)
					throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.reduce(arg0, arg1, context);
		for (Person person : arg1) {
			context.write(new Text(arg0.getLastName()), person);
		}
	}*/
	
}
