/* 
Kevin Nguyen
kdn433
CS378 - Big Data Programming - Project 5
*/
package com.refactorlabs.cs378.assign5;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
 */
public class UserSession extends Configured implements Tool {
	/**
	 * The Reduce class
	 */
	public static class ReduceClass extends Reducer<Text, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {
		/* Reduce class */
		public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context) throws IOException, InterruptedException {
			/* Local Declarations */
			boolean first = true; //user-id will be the same per set of sessions
			HashSet<Event> myEList = new HashSet<Event>(); //list of event obj's
			int counter = 0;
			Session.Builder mySession = Session.newBuilder(); //session instance
			// /* Loop through each session object */
			for (AvroValue<Session> value : values) {
				/* user-id is always identical due to how hadoop works */
				if (first) {
					CharSequence myId = value.datum().getUserId();
					mySession.setUserId(myId);
					first = false;
				}
				myEList.add(value.datum().getEvents().get(0));
			}
			ArrayList<Event> newList = new ArrayList<Event>();
			/* add members of set to list */
			for(Event thisE : myEList) {
				newList.add(thisE);
			}
			/* sort elements */
			Collections.sort(newList);
			/* put list inside session instance */
			mySession.setEvents(newList);
			// /* write context */
			context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<Session>(mySession.build()));
		}
	}

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
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(UserSessionMapper.class);
		job.setMapOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

		// Specify the Reduce
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(ReduceClass.class);
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(job, Session.getClassSchema());

		// Grab the input file and output directory from the command line.
		String[] inputPaths = appArgs[0].split(",");
		for ( String inputPath : inputPaths ) {
			FileInputFormat.addInputPath(job, new Path(inputPath));
		}
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
		int res = ToolRunner.run(new UserSession(), args);
		System.exit(res);
	}
}