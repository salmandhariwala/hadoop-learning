package problem.first.CustWithMaxSpendingByMonth;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 * this class get input from mapper and this class will decide where to send this entry i mean which reducer
 * 
 * in our example number of reducer is 4
 * 
 * 
 */
public class MaxSpendPartitioner extends Partitioner<TextPair, LongWritable> {

	@Override
	public int getPartition(TextPair key, LongWritable val, int numPartitions) {

		String date = key.getFirst().toString().substring(0, 4);
		String month = key.getFirst().toString().substring(5, 7);

		int partition = ((date.hashCode() + month.hashCode()) % numPartitions);

		return partition;
	}

}
