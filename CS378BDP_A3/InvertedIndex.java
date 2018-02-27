/* 
Kevin Nguyen
UTEID: kdn433
*/

package com.refactorlabs.cs378.assign3;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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

/**
 * Example MapReduce program that performs word count.
 * Modified by Kevin Nguyen, kdn433 for CS378 Big Data Programming - Project 3.
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class InvertedIndex extends Configured implements Tool {

	/* Map Class to create the inverted index of a word key and a list of docs as value */
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text idEmail = new Text();
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			/* Local declarations */
			String line = value.toString(); //first line of email
			String temp = ""; //temp string
			StringTokenizer tokenizer = new StringTokenizer(line); //break line by tokens 			boolean isEmail = false, isKey = false; //value and key boolean
			/* loop through each word in line and parse the email information; format for output */
			while(tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken(); //obtain token
				boolean isComplete = false; //check if context can be written
				/* check if token is worthy for parsing */
				if (token.contains("Subject:") || token.contains("Mime-Version:") || token.contains("X-From:")) {
					break;
				}
				else {
					if (isIDEmail(token)) {
						String wordValue = tokenizer.nextToken(); //get next token idEmail
						idEmail.set(wordValue); //set the value
					}
					else if (isKey(token)) {
						temp = token;
						String nextToken = tokenizer.nextToken(); //get next token email
						/* remove the comma from this token */
						if (nextToken.contains(",")) {
							nextToken = nextToken.replace(",","");
						}
						String wordKey = temp + "" + nextToken; //merge the type with email
						isComplete = true; //set condition to write context
						word.set(wordKey); //set the key
					}
					else if (token.contains("@") && !temp.equals("")) {
						/* remove extra commas */
						if (token.contains(",")) {
							token = token.replace(",","");
						}
						String wordKey = temp + "" + token; //merge the type with email
						isComplete = true; //set condition to write context
						word.set(wordKey); //set the key
					}		
					/* if final string is complete then write to context */
					if (isComplete) {
						context.write(word, idEmail); //format = [email_reference, ID_Email_1]
					}
				}
			}
		}

		/* validate if the token is an email */
		public boolean isKey(String token) {
			/* if line contains specific identifier then it is an email for key */
			if (token.contentEquals("From:") || token.contentEquals("To:") || token.contentEquals("Cc:") || token.contentEquals("Bcc:")) {
				return true;
			}
			return false;
		}

		/* Validate token for message id identififer */
		public boolean isIDEmail(String token) {
			/* checks token if type is message id */
			if (token.contentEquals("Message-ID:")) {
				return true;
			}
			return false;
		}
	}

	/* Reducer class that will group up related indexes and ??? */
	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		/* global class declarations */
		String emailValue = "", temp = "";
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			/* local declarations */
			String result = "";
			int count = 0;
			/* Store in set to avoid duplicates */
			Set<String> wordSet = new TreeSet<String>();
			/* iterate through each value and store into a colleciton set */
			for (Text value : values) {
				String val = value.toString(); //temporary string to hold data
				wordSet.add(val);
			}
			/* get array from set */
			List<String> mailIDList = new ArrayList<String>(wordSet);
			/* iterate through array and form result string */
			while (count < mailIDList.size()) {
				result += mailIDList.get(count) + ",";
				count++;
			}
			result = result.substring(0,result.lastIndexOf(","));
			context.write(key, new Text(result)); //context write the final info
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

		Job job = Job.getInstance(conf, "InvertedIndex");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(InvertedIndex.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
        job.setCombinerClass(ReduceClass.class);

		// Set the input and output file formats.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPath(job, new Path(appArgs[0]));
		FileInputFormat.addInputPath(job, new Path(appArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);
		return 0;
	}

	/* main function for execution */
	public static void main(String[] args) throws Exception {
		Utils.printClassPath();
		int res = ToolRunner.run(new InvertedIndex(), args);
		System.exit(res);
	}
}