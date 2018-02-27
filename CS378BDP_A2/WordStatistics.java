/* 
Kevin Nguyen
UTEID: kdn433
kxnguyen60@utexas.edu
Project 2 - CS378 - Big Data Programming

Project design by Dr. Franke, UT Austin.
*/

package com.refactorlabs.cs378.assign2;

import com.refactorlabs.cs378.assign2.WordStatisticsWritable;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.*;
import java.util.*;
import java.net.URL;
import java.net.URLClassLoader;

public class WordStatistics extends Configured implements Tool {
	/* MAP CLASS for WordStatistics with the goal to look at each word and increment the count of it */
	public static class MapClass extends Mapper<LongWritable, Text, Text, WordStatisticsWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			/* hashmap to keep track of counts */
			HashMap<String, Double> wordMap = new HashMap<String,Double>();
			/* get text line */
			String line = value.toString();
			/* APPLY FILTER */
			line = parseLine(line);
			/* string tokenzier start */
			StringTokenizer tokenizer = new StringTokenizer(line);
			// For each word in the input line store into array ?
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken(); //get word
				/* case to store into map */
				if (wordMap.containsKey(token)) {
					/* word exists so update class fields and structures */
					wordMap.put(token, wordMap.get(token)+1.0);
				}
				else {
					/* word doesn't exist? */
					wordMap.put(token, 1.0);
				}
			}
			/* New wsw object initialized and then set */
			setCountsAllData(wordMap, context);
		}
	

		/* iterate through wordmap and count the frequencies and set in wsw. */
		public void setCountsAllData(HashMap<String, Double> wordMap, Context context) throws IOException, InterruptedException {
			String tempKey = "";
			Double tempVal = 0.0;
			/* loop and set field counts (if any) from map object */
			for (Map.Entry<String, Double> entry : wordMap.entrySet()) {
				WordStatisticsWritable wsw = new WordStatisticsWritable();
				/* key and value */
				tempKey = entry.getKey();
				tempVal = entry.getValue();
				/* set values to wsw */
				wsw.setOccurance(tempVal);
				wsw.setOccurance_squared(tempVal*tempVal);
				wsw.setOccurance_counter(1.0);
				context.write(new Text(tempKey), wsw); //context write
			}
		}

		/* filter all punctuations in line with replace */
		public String parseLine(String line) {
			line = line.toLowerCase();
			line = line.replace(".","");
			line = line.replace(",","");
			line = line.replace("?","");
			line = line.replace("!","");
			line = line.replace("(","");
			line = line.replace(")","");
			line = line.replace(";","");
			line = line.replace("_","");
			line = line.replace("1","");
			line = line.replace("2","");
			line = line.replace("3","");
			line = line.replace("4","");
			line = line.replace("5","");
			line = line.replace("\"","");
			return line;
		}
	}

	/* COMBINER HERE */
	public static class CombineClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {
		/* combine all relevant tokens and add counts here. Also update fields in wsw as needed */
		public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context) throws IOException, InterruptedException {
			Double occurances = 0.0, occurances_squared = 0.0, occurances_counter = 0.0;
			/* group together common tokens and add up all counter types */
			for (WordStatisticsWritable myData : values) {
				occurances += myData.occurances;
				occurances_squared += myData.occurances_squared;
				occurances_counter += myData.occurances_counter;
			}
			// /* store value again into wsw */
			WordStatisticsWritable wsw = new WordStatisticsWritable();
			wsw.setOccurance(occurances);
			wsw.setOccurance_squared(occurances_squared);
			wsw.setOccurance_counter(occurances_counter);
			wsw.setMean(0.0);
			wsw.setVariance(0.0);
			context.write(key, wsw);  //write context
		}
	}

	/* REDUCER with the goal of using the data that was created during MAP process to compute mean and variance */
	public static class ReduceClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {
		@Override
		public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context) throws IOException, InterruptedException {
			Double occurances = 0.0, occurances_squared = 0.0, occurances_counter = 0.0, mean = 0.0, variance = 0.0;
			/* group together common tokens and add up all counter types */
			for (WordStatisticsWritable myData : values) {
				occurances += myData.getOccurance();
				occurances_squared += myData.getOccurance_squared();
				occurances_counter += myData.getOccurance_counter();
			}
			/* obtain mean and variance */
			mean = getMean(occurances, occurances_counter);
			variance = getVariance(occurances_squared, occurances_counter, mean);
			/* new writable */
			WordStatisticsWritable wsw = new WordStatisticsWritable();
			/* set counts, mean and variance to wsw */
			wsw.setOccurance(occurances);
			wsw.setOccurance_squared(occurances_squared);
			wsw.setOccurance_counter(occurances_counter);
			wsw.setMean(mean);
			wsw.setVariance(variance);
			context.write(key, wsw);  //context write
		}	

		/* return the mean value */
		public Double getMean(Double o, Double oc) {
			return (o/oc);
		}

		/* return the variance value */
		public Double getVariance(Double os, Double oc, Double m) {
			return ((os/oc) - (m*m));
		}
	}

	/**
	 * The run method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "WordStatistics"); //new job
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordStatistics.class); // <------------------------------ WHAT IS THIS FOR??

		// Set the input and output file formats.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// Set the output key and value types (for map and reduce).
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WordStatisticsWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WordStatisticsWritable.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(CombineClass.class);
		job.setReducerClass(ReduceClass.class);

		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPath(job, new Path(appArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);
		return 0;
	}

	/**
	 * The main method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public static void main(String[] args) throws Exception {
		Utils.printClassPath();
		int res = ToolRunner.run(new WordStatistics(), args);
		System.exit(res);
	}

}