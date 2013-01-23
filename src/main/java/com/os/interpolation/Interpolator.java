package com.os.interpolation;


import com.os.measurement.TimedValue;

import java.util.List;

/**
 * @author Vadim Bobrov
 */
public interface Interpolator {
	List<TimedValue> offer(TimedValue tv);
}
