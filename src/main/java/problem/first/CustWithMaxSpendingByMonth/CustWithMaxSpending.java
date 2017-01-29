/* Customer with Maximum Spending By Month */

package problem.first.CustWithMaxSpendingByMonth;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustWithMaxSpending {
	public static void main(String[] args) throws Exception {
		/*
		 * declare config object
		 */
		Configuration conf = new Configuration();

		/*
		 * declare job object
		 */
		Job job = Job.getInstance(conf, "CustWithMaxSpending");

		/*
		 * set main class for this job
		 */
		job.setJarByClass(CustWithMaxSpending.class);

		/*
		 * set input file type for this job
		 */
		job.setInputFormatClass(TextInputFormat.class);

		/*
		 * define input paths along with their data type and their mapper
		 */
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MaxSpendMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MaxSpendMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, MaxSpendMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, MaxSpendMapper.class);

		/*
		 * set mapper output key and value
		 */
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(LongWritable.class);

		/*
		 * set number of reducer
		 */
		job.setNumReduceTasks(4);

		/*
		 * Define the comparator that controls how the keys are sorted before
		 * they are passed to the Reducer.
		 */
		job.setSortComparatorClass(SortComparator.class);

		/*
		 * Define the comparator that controls which keys are grouped together
		 * for a single call to Reducer
		 * 
		 */
		job.setGroupingComparatorClass(GroupingComparator.class);

		/*
		 * set partitioner of job
		 * 
		 * partitionar will chose which reducer to send data
		 */
		job.setPartitionerClass(MaxSpendPartitioner.class);

		/*
		 * set reducer for this job
		 */
		job.setReducerClass(MaxSpendReducer.class);

		/*
		 * set output key data type and value datatype
		 */
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		/*
		 * define output directory
		 */
		FileOutputFormat.setOutputPath(job, new Path(args[4]));

		/*
		 * await termmination for this job
		 */
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

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
class MaxSpendMapper extends Mapper<LongWritable, Text, TextPair, LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		/*
		 * input line from file
		 */
		String[] line = value.toString().split(";");

		/*
		 * get date
		 */
		String date1 = line[0].substring(0, 10);

		/*
		 * get customer id
		 */
		String custID = line[1].substring(0, 8);

		/*
		 * get amount
		 */
		long amt = Long.parseLong(line[8]);

		/*
		 * write textpair (date,custid) as key and value will be amount
		 */
		context.write(new TextPair(new Text(date1), new Text(custID)), new LongWritable(amt));
	}
}

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
class MaxSpendReducer extends Reducer<TextPair, LongWritable, LongWritable, Text> {

	private TreeMap<Long, String> countMap = new TreeMap<Long, String>();

	/*
	 * input will be like this
	 * 
	 * key : textpair (date,custid)
	 * 
	 * value (list) : amount spent
	 *
	 */

	@Override
	public void reduce(TextPair key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		/*
		 * make sum of spending
		 */
		long sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
		}

		/*
		 * extract date and custid
		 * 
		 * club them in new var
		 * 
		 */
		String date = key.getFirst().toString().substring(0, 7);
		String custId = key.getSecond().toString();
		String custId_Date = custId + "\t" + date;

		/*
		 * make entry of this in countmap
		 */
		countMap.put(sum, custId_Date);

	}

	/*
	 * 
	 * this method get call when reducer is done with all processing
	 * 
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {

		/*
		 * get last entry of map as it will have max amount
		 * 
		 * 
		 */
		long MaxAmount = countMap.pollLastEntry().getKey();
		String custIdDt = countMap.pollLastEntry().getValue().toString();

		/*
		 * write to context for output
		 */
		context.write(new LongWritable(MaxAmount), new Text(custIdDt));

	}
}
