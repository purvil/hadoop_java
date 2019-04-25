package org.hadoop.trainings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.WritableComparable;

public class WCWritable implements WritableComparable<WCWritable>{
	private String word;
	
	public WCWritable() {
		set("");
	}
	
	public WCWritable(String word) {
		set(word);
	}
	
	public void set(String word) {
		this.word = word;
	}
	
	public String getWord() {
		return this.word;
	}
	
	// How to write values to byte stream
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, this.word);
	}
	
	// How to read byte stream values.
	public void readFields(DataInput in) throws IOException {
		this.word = WritableUtils.readString(in);
	}
	
	public int compareTo(WCWritable o) {
		return this.word.compareTo(o.getWord());
	}
	
	public String toString() {
		return this.word;
	}
}
