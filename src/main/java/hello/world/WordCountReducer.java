package hello.world;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/*
 * 
 * param 1 : input key class
 * 
 * param 2 : input value class which will be list
 * 
 * param 3 : output key class
 * 
 * param 4 : output value class
 * 
 * 
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	
	public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
		
		/*
		 * loop over values of input and sum them up
		 */
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		
		/*
		 * write back the result in context
		 */
		con.write(word, new IntWritable(sum));
		
		
	}
}
