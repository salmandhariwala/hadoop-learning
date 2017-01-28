package hello.world;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	public static void main(String[] args) throws Exception {

		/*
		 * declare config object this object will get all config eg name node
		 * address etc
		 */
		Configuration c = new Configuration();

		/*
		 * declare input and output path
		 */
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		/*
		 * declare instance of job pass config and job name
		 */
		Job job = Job.getInstance(c, "wordcount");

		/*
		 * main class from where execution will start
		 */
		job.setJarByClass(WordCount.class);

		/*
		 * mapper of the job
		 */
		job.setMapperClass(WordCountMapper.class);

		/*
		 * reducer of the class
		 */
		job.setReducerClass(WordCountReducer.class);

		/*
		 * set output key and value class
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/*
		 * declare input path and output path
		 */
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		/*
		 * wait for termination
		 */
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
