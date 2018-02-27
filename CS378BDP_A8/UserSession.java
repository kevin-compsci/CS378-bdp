/* 
Kevin Nguyen
kdn433
CS378 - Big Data Programming - Project 7
*/
package com.refactorlabs.cs378.assign6;

import com.refactorlabs.cs378.sessions.*;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
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
import org.apache.hadoop.util.ToolRunner;
import java.io.*;
import java.util.*;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * WordCount example using Avro defined class for the word count data,
 * to demonstrate how to use Avro defined objects.
 *
 * Here the reduce outputs (key and value) are both Avro objects, written as text.
 */
public class UserSession extends Configured implements Tool {
	/**
	 * This class defines the reduce() function
	 */
	// public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
	// 	/* Reduce class */
	// 	/* EMPTY */
	// }

	/**
	 * The run() method is called (indirectly) from main(), and contains all the job
	 * setup and configuration.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: UserSession <input path> <output path>");
			return -1;
		}

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "UserSession");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(UserSession.class);

		// Specify the Map
		job.setInputFormatClass(AvroKeyValueInputFormat.class);
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		job.setMapperClass(UserSessionMapper.class); //CHANGE?
		//job.setMapOutputKeyClass(Text.class);
		//AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

		AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job, Session.getClassSchema());

		// Specify the Reduce
		// job.setOutputFormatClass(TextOutputFormat.class);
		// job.setReducerClass(ReduceClass.class);
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(Text.class);

		// Grab the input file and output directory from the command line.
		FileInputFormat.setInputPaths(job, new Path(appArgs[0])); 
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		/* Outputs */
		AvroMultipleOutputs.addNamedOutput(job, SessionType.SUBMITTER.getText(), AvroKeyValueOutputFormat.class, Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, SessionType.CLICKER.getText(), AvroKeyValueOutputFormat.class, Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, SessionType.SHOWER.getText(), AvroKeyValueOutputFormat.class, Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, SessionType.VISITOR.getText(), AvroKeyValueOutputFormat.class, Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, SessionType.OTHER.getText(), AvroKeyValueOutputFormat.class, Session.getClassSchema());


		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(job, Session.getClassSchema());


		/* set counters enabled */
		AvroMultipleOutputs.setCountersEnabled(job, true);

		/* set reduce to 0 */
		job.setNumReduceTasks(0);

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
		int res = ToolRunner.run(new UserSession(), args);
		System.exit(res);
	}
}