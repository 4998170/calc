package com.outsmart;

import com.outsmart.interpolation.Interpolator;
import com.outsmart.interpolation.SlidingInterpolatorImpl;
import com.outsmart.measurement.TimedValue;
import com.outsmart.measurement.TimedValueWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Vadim Bobrov
 */
public class InterpolatorJob extends Configured implements Tool {

	public static class MyMapper extends TableMapper<
			ImmutableBytesWritable,		// customer, location, wireid
			TimedValueWritable          // timestamp and value to be interpolated
			> {

		private int numRecords = 0;

		@Override
		public void map(ImmutableBytesWritable rowkey, Result rowvalue, Context context) throws IOException, InterruptedException {

			byte[] customerHash = RowKeyUtil.getCustomerHash(rowkey.get());
			byte[] locationHash = RowKeyUtil.getLocationHash(rowkey.get());
			byte[] wireidHash = RowKeyUtil.getWireIdHash(rowkey.get());

			ImmutableBytesWritable wireKey = new ImmutableBytesWritable(Bytes.add(customerHash, locationHash, wireidHash));

			long timestamp = RowKeyUtil.getTimestamp(rowkey.get());
			double energy = Bytes.toDouble(rowvalue.getValue(Bytes.toBytes(Settings.ColumnFamilyName), Bytes.toBytes(Settings.EnergyQualifierName)));

			try {
				context.write(wireKey, new TimedValueWritable(timestamp, energy));
			} catch (InterruptedException e) {
				throw new IOException(e);
			}

			if ((numRecords++ % 10000) == 0)
				context.setStatus("mapper processed " + numRecords + " records so far");

		}

	}

	protected static enum InterpolatorValue {
		INCOMING_VALUES,
		INCOMING_KEYS
	}

	public static class MyReducer extends TableReducer<ImmutableBytesWritable, TimedValueWritable, ImmutableBytesWritable> {

		private Log log = LogFactory.getLog(MyReducer.class);

		@Override
		public void reduce(ImmutableBytesWritable wireKey, Iterable<TimedValueWritable> timedValues, Context context) throws IOException, InterruptedException {

			Interpolator itp = new SlidingInterpolatorImpl();

			List<TimedValue> tvalues = new ArrayList<TimedValue>();

			for(TimedValueWritable tvw : timedValues) {
				log.info("received value " + tvw.getTimedValue().timestamp());
				tvalues.add(tvw.getTimedValue());
			}
			Collections.sort(tvalues);

			//context.getCounter(InterpolatorValue.INCOMING_KEYS).increment(1);

			//int i = 0;
			log.info("iterating over timed values for " + wireKey.get());
			for(TimedValue tvw : tvalues) {
				//i++;
				log.info("timed value " + tvw.timestamp());
				//context.getCounter(InterpolatorValue.INCOMING_VALUES).increment(1);
				//TODO: make sure they are sorted by time
				List<TimedValue> interpolated = itp.offer (tvw);

				for(TimedValue tv : interpolated) {
					log.info("\tinterpolated value " + tv.timestamp());
					// create key with interpolated timestamp
					ImmutableBytesWritable itpKey = new ImmutableBytesWritable(Bytes.add(wireKey.get(), Bytes.toBytes(Long.MAX_VALUE - tv.timestamp())));

					Put put = new Put(itpKey.get());
					put.add(Bytes.toBytes(Settings.ColumnFamilyName), Bytes.toBytes(Settings.EnergyQualifierName), Bytes.toBytes(tv.value()));

					context.write(itpKey, put);
				}
			}

			//log.info("iterated over " + i + " records");

		}
	}


	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "InterpolatorJob");
		job.setJarByClass(InterpolatorJob.class);

		Scan scan = new Scan();
		scan.setCaching(500);        					// 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  					// don't set to true for MR jobs
		scan.addColumn(Bytes.toBytes(Settings.ColumnFamilyName), Bytes.toBytes(Settings.EnergyQualifierName));

		TableMapReduceUtil.initTableMapperJob(
				Settings.TableName,						// input HBase table name
				scan,             						// Scan instance to control CF and attribute selection
				MyMapper.class,   						// mapper
				ImmutableBytesWritable.class,	        // mapper output key
				TimedValueWritable.class,        			// mapper output value
				job);

		TableMapReduceUtil.initTableReducerJob(
				Settings.MinuteInterpolaedTableName,	// output HBase table name
				MyReducer.class,
				job);

		job.setNumReduceTasks(1);   					// at least one, adjust as required
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new InterpolatorJob(), args);
		System.exit(res);
	}
}