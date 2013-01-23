package com.os.measurement;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Vadim Bobrov
 */

public class TimedValueWritable implements WritableComparable<TimedValueWritable> {

	private TimedValue timedValue;

	public TimedValueWritable() {}

	public TimedValueWritable(long timestamp, double value) {
		this.timedValue = new TimedValue(timestamp, value);
	}

	public TimedValue getTimedValue() {
		return timedValue;
	}

	@Override
	public int compareTo(TimedValueWritable o) {
		return this.timedValue.compareTo(o.getTimedValue());
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(timedValue.timestamp());
		dataOutput.writeDouble(timedValue.value());
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		timedValue = new TimedValue(dataInput.readLong(), dataInput.readDouble());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		TimedValueWritable that = (TimedValueWritable) o;

		if (timedValue != null ? !timedValue.equals(that.timedValue) : that.timedValue != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return timedValue != null ? timedValue.hashCode() : 0;
	}
}
