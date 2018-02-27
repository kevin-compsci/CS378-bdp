/* 
Kevin Nguyen
UTEID: kdn433
kxnguyen60@utexas.edu
Project 2 - CS378 - Big Data Programming

Project design by Dr. Franke, UT Austin.
*/


package com.refactorlabs.cs378.assign2;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ToolRunner;
import java.io.*;
import java.util.*;
import java.net.URL;
import java.net.URLClassLoader;


public class WordStatisticsWritable implements Writable {
	/* fields for data reference */	
	double occurances;
	double occurances_squared;
	double occurances_counter;
	double mean;
	double variance;


	/* constructor no params */
	public WordStatisticsWritable() {
	}

	/* constructor no params */
	public WordStatisticsWritable(double o, double os, double oc, double m, double v) {
		this.occurances = o;
		this.occurances_squared = os;
		this.occurances_counter = oc;
		this.mean = m;
		this.variance = v;
	}

	/* serialize data input */
	public void readFields(DataInput in) throws IOException {
		Double v = 0.0;
		v = in.readDouble();
		setOccurance(v);
		v = in.readDouble();
		setOccurance_squared(v);
		v = in.readDouble();
		setOccurance_counter(v);
		v = in.readDouble();
		setMean(v);
		v = in.readDouble();
		setVariance(v);
	}

	/* serialize data output */
	public void write(DataOutput out) throws IOException {
		out.writeDouble(getOccurance());
		out.writeDouble(getOccurance_squared());
		out.writeDouble(getOccurance_counter());
		out.writeDouble(getMean());
		out.writeDouble(getVariance());
	}


	/* set occurance value */
	public void setOccurance(Double o) {
		this.occurances = o;
	}

	/* set occurance_squared value */
	public void setOccurance_squared(Double os) {
		this.occurances_squared = os;
	}

	/* set occurance_counter value */
	public void setOccurance_counter(Double oc) {
		this.occurances_counter = oc;
	}

	/* get occurance value */
	public Double getOccurance() {
		return this.occurances;
	}

	/* get occurance_squared value */
	public Double getOccurance_squared() {
		return this.occurances_squared;
	}

	/* get occurance_counter value */
	public Double getOccurance_counter() {
		return this.occurances_counter;
	}

	/* set mean */
	public void setMean(Double m) {
		this.mean = m;
	}

	/* set variance */
	public void setVariance(Double v) {
		this.variance = v;
	}

	/* get mean */
	public Double getMean() {
		return this.mean;
	}

	/* get vriance */
	public Double getVariance() {
		return this.variance;
	}

	/* toString to output the data */
	public String toString() {
		return  getOccurance_counter() + ", " + getMean() + ", " + getVariance();
	}
}

/*
himself	12,1.4166666666666667,0.4097222222222219
hindered	2,1.0,0.0
his	35,2.8857142857142857,3.9297959183673488
history	5,1.2,0.16000000000000014
hitherto	1,1.0,0.0
hoarded	1,1.0,0.0
hobgoblin	1,1.0,0.0

*/