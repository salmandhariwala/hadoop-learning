package hello.world;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * param 1 : not known
 * 
 * param 2 : input line from the file
 * 
 * param 3 : ouput key
 * 
 * param 4 : output value
 * 
 * 
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

		/*
		 * read the line
		 */
		String line = value.toString();

		/*
		 * split this line in words
		 */
		String[] words = line.split(" ");

		/*
		 * iterate over words
		 */
		for (String word : words) {

			/*
			 * form the key using word
			 */
			Text outputKey = new Text(word.toUpperCase().trim());

			/*
			 * form the value which is hardcoded 1
			 */
			IntWritable outputValue = new IntWritable(1);

			/*
			 * write these key values to context which we will access later
			 */
			con.write(outputKey, outputValue);

		}
	}
}
