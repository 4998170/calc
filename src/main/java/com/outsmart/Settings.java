package com.outsmart;

/**
 * @author Vadim Bobrov
 */
public class Settings {

	/*
	  You may need to find a sweet spot between a low number of RPCs and the memory
	  used on the client and server. Setting the scanner caching higher will improve scanning
		performance most of the time, but setting it too high can have adverse effects as well:
		each call to next() will take longer as more data is fetched and needs to be transported
		  to the client, and once you exceed the maximum heap the client process has available
		it may terminate with an OutOfMemoryException.
	  When the time taken to transfer the rows to the client, or to process the
		data on the client, exceeds the configured scanner lease threshold, you
	  will end up receiving a lease expired error, in the form of a Scan
		nerTimeoutException being thrown.
	  */
	static public final int ScanCacheSize = 1000;     		   			// how many rows are retrieved with every RPC call


	static public final String  TableName = "msmt";              		// table for actual measurements
	static public final String MinuteInterpolaedTableName = "ismt";    // table for minute interpolation
	static public final String RollupTableName = "rsmt";    			// table for minute rollup by location


	static public final String ColumnFamilyName = "d";      			// stands for data
	static public final String EnergyQualifierName = "e";   			// stands for energy
	static public final String CurrentQualifierName = "c";  			// stands for current
	static public final String VampireQualifierName = "v";  			// stands for volt-amp-reactive

	static public final int  BatchSize = 1000;             			// default writer batch size - can be lost
	static public final int  DerivedDataBatchSize = 10;				// writer batch size for minute interpolations- can be lost

	static public final int  TablePoolSize = 100;

	static public final String Host = "192.168.152.128";
		//val Host = "10.0.0.158"

	static public final int  ExpiredTimeWindow = 570000;				// time to incoming measurement expiration in milliseconds
																// measurements older than that are not interpolated
}
