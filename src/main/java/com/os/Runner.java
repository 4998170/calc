package com.os;

import com.os.job.InterpolatorJob;
import com.os.job.RollupJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Vadim Bobrov
 */
public class Runner  extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		boolean res = InterpolatorJob.getJob().waitForCompletion(true);
		if(res)
			res = RollupJob.getJob().waitForCompletion(true);

		System.exit(res ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Runner(), args);
		System.exit(res);
	}

}
