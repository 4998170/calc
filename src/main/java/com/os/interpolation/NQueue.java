package com.os.interpolation;


import com.os.measurement.TimedValue;

/**
 * @author Vadim Bobrov
 */
public interface NQueue {
	TimedValue get(int i);

	boolean isFull();

	void offer(TimedValue tv);
}
