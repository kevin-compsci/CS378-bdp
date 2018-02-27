/* 
Kevin Nguyen
kdn433
CS378 - Big Data Programming - Project 6
*/


/*
OUTPUT SNIPPET A6:
1C4AJWAG8DL655012   {"unique_users": 1, "clicks": {}, "edit_contact_form": 0, "marketplace_srps": 6, "marketplace_vdps": 0}
1C4BJWDG8HL501891   {"unique_users": 2, "clicks": {"GET_DIRECTIONS": 1, "FEATURES": 1}, "edit_contact_form": 0, "marketplace_srps": 3, "marketplace_vdps": 0}
1C4BJWEG0CL227920   {"unique_users": 1, "clicks": {"FEATURES": 1}, "edit_contact_form": 0, "marketplace_srps": 0, "marketplace_vdps": 0}
1C4BJWEG3EL300085   {"unique_users": 2, "clicks": {"CONTACT_BANNER": 1}, "edit_contact_form": 0, "marketplace_srps": 2, "marketplace_vdps": 0}
1FT8W3BT3BEA00185   {"unique_users": 1, "clicks": {"GET_DIRECTIONS": 1}, "edit_contact_form": 0, "marketplace_srps": 3, "marketplace_vdps": 1}
1FT8W3CT4BEA37955   {"unique_users": 1, "clicks": {}, "edit_contact_form": 0, "marketplace_srps": 6, "marketplace_vdps": 3}
*/

package com.refactorlabs.cs378.assign5;
import com.refactorlabs.cs378.sessions.*;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.*;
import java.util.*;
import java.net.URL;
import java.net.URLClassLoader;

/* main class */
public class UserSession extends Configured implements Tool {
	/**
	 * This class defines the reduce() function
	 */
	public static class ReduceClass extends Reducer<Text, AvroValue<VinImpressionCounts>, Text, AvroValue<VinImpressionCounts>> {
		/* Reduce class */
		public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context) throws IOException, InterruptedException {
			/* Local Declarations */
			HashMap<CharSequence, AvroValue<VinImpressionCounts>> sessionMap = new HashMap<CharSequence, AvroValue<VinImpressionCounts>>();
			HashMap<CharSequence, AvroValue<VinImpressionCounts>> impMap = new HashMap<CharSequence, AvroValue<VinImpressionCounts>>();
			Boolean first = true;		
			/* new vic object */
		    VinImpressionCounts.Builder newVIC = VinImpressionCounts.newBuilder();
		    Map<CharSequence, Long> subtypeMap = new HashMap<CharSequence, Long>();
		    newVIC.setClicks(subtypeMap);
		    /* iterate over all VIC's to get Session and Impression data */
		 	for (AvroValue<VinImpressionCounts> myVIC : values) {
				String myVIN = key.toString(); //get vin;
				/* get information of incoming vic object */
				long myCounts = myVIC.datum().getUniqueUsers(); //get unique users
				long myECF = myVIC.datum().getEditContactForm(); //get editcontactform
				long mySRP = myVIC.datum().getMarketplaceSrps(); //get marketplacesrps
				long myVDP = myVIC.datum().getMarketplaceVdps(); //get marketplacevdps
				Map<CharSequence, Long> myClicks = myVIC.datum().getClicks(); //get clicks map
				/* set counters */
				newVIC.setUniqueUsers(newVIC.getUniqueUsers()+myCounts);
				newVIC.setEditContactForm(newVIC.getEditContactForm()+myECF);
				newVIC.setMarketplaceSrps(newVIC.getMarketplaceSrps()+mySRP);
				newVIC.setMarketplaceVdps(newVIC.getMarketplaceVdps()+myVDP);
				/* sort between SessionMap and impressionMap */
				if((mySRP < 1 && myVDP < 1)) {
					if(!myClicks.isEmpty()) {
						for(Map.Entry<CharSequence, Long> entry : myClicks.entrySet()) {
							CharSequence subT = entry.getKey();
							Long subV = entry.getValue();
							/* consider adding to final vic */
							if(newVIC.getClicks().get(subT) == null) { //does not exists
								Map<CharSequence, Long> temp = newVIC.getClicks();
								temp.put(subT, subV);
								newVIC.setClicks(temp);
							}
							else { //exists
								Map<CharSequence, Long> temp = newVIC.getClicks();
								Long r = temp.get(subT)+subV;
								temp.put(subT, subV);
								newVIC.setClicks(temp);
							}
						}
					}
					sessionMap.put(myVIN, new AvroValue<VinImpressionCounts>(newVIC.build()));	
					//context.write(key, myVIC);
				}
				else {
					impMap.put(myVIN, new AvroValue<VinImpressionCounts>(newVIC.build()));
				}
		 	}
		 	/* session map */
			for(Map.Entry<CharSequence, AvroValue<VinImpressionCounts>> left : sessionMap.entrySet()) {
				String leftKey = left.getKey().toString();
				AvroValue<VinImpressionCounts> leftValue = left.getValue();
				/* iterate through right side to find comparisons and update with left side */
				for(Map.Entry<CharSequence, AvroValue<VinImpressionCounts>> right : impMap.entrySet()) {
					String rightKey = right.getKey().toString();
					AvroValue<VinImpressionCounts> rightValue = right.getValue();
					/* update left side with right side information */
					if(leftKey.equals(rightKey)) { //key VINS are the same
						leftValue.datum().setMarketplaceSrps(rightValue.datum().getMarketplaceSrps()); //combine left with right marketplacesrp value
						leftValue.datum().setMarketplaceVdps(rightValue.datum().getMarketplaceVdps()); //combine left with right marketplacevdp value
						// //NOTE: right side impression map will only have uniqueusers and marketplace data
						// //NOTE2: left side session map will have everything but marketplace data --> it is 0
					}
				}
				/* write data */
				context.write(new Text(leftKey), leftValue);
			}
	 	}
	}

	/**
	 * The run() method is called (indirectly) from main(), and contains all the job
	 * setup and configuration.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage: UserSession <input path> <output path>");
			return -1;
		}
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "UserSession");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(UserSession.class);
		// Specify the Map
		job.setMapperClass(UserSessionMapper.class); //CHANGE?
		// Specify the Reduce
		job.setReducerClass(ReduceClass.class);
		//input avro mapper format
		AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job, Session.getClassSchema());	
		//output mapper format
		job.setMapOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());
		//output reducer format
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		AvroJob.setOutputValueSchema(job, VinImpressionCounts.getClassSchema());
		// Grab the input file and output directory from the command line.
		MultipleInputs.addInputPath(job, new Path(appArgs[0]), AvroKeyValueInputFormat.class, UserSessionMapper.class);
		MultipleInputs.addInputPath(job, new Path(appArgs[1]), TextInputFormat.class, UserImpressionMapper.class); 
		MultipleInputs.addInputPath(job, new Path(appArgs[2]), TextInputFormat.class, UserImpressionMapper.class); 
		FileOutputFormat.setOutputPath(job, new Path(appArgs[3]));
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