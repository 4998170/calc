package com.os.measurement;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * @author Vadim Bobrov
 */
public class TimedValueWritableTest {
	@Test
	public void testCompareTo() throws Exception {

		TimedValueWritable[] arr = new TimedValueWritable[]{new TimedValueWritable(3, 3), new TimedValueWritable(1, 1), new TimedValueWritable(2, 2)};
		Arrays.sort(arr);
		assertEquals(arr[0].getTimedValue().timestamp(), 1);
		assertEquals(arr[1].getTimedValue().timestamp(), 2);
		assertEquals(arr[2].getTimedValue().timestamp(), 3);
	}

	@Test
	public void testCompareToTimedValue() throws Exception {

		TimedValue[] arr = new TimedValue[]{new TimedValue(3, 3), new TimedValue(1, 1), new TimedValue(2, 2)};
		Arrays.sort(arr);
		assertEquals(arr[0].timestamp(), 1);
		assertEquals(arr[1].timestamp(), 2);
		assertEquals(arr[2].timestamp(), 3);
	}

}
