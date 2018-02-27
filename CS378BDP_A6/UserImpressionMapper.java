/* 
Kevin Nguyen
kdn433
CS378 - Big Data Programming - Project 6
*/

package com.refactorlabs.cs378.assign5;

import com.refactorlabs.cs378.sessions.*;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.*;

/**
 * Map class reads csv files
 */
public class UserImpressionMapper extends Mapper<LongWritable, Text, Text, AvroValue<VinImpressionCounts>> {
    /* map class */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /* Local declarations */
        String row = value.toString();
        String[] columns = row.split(",");
        VinImpressionCounts.Builder myVIC = VinImpressionCounts.newBuilder();
        /* get VIN */
        String myVIN = columns[0];
        /* get count */
        long count = Long.parseLong(columns[2]);
        /* get Impression data */
        String myDATA = columns[1];
        if(myDATA.equals("VDP")) {
            myVIC.setMarketplaceVdps(count);
        }
        else if(myDATA.equals("SRP")) {
            myVIC.setMarketplaceSrps(count);
        }
        /* initialize click map to be empty */
        Map<CharSequence, Long> subtypeMap = new HashMap<CharSequence, Long>();
        myVIC.setClicks(subtypeMap); //set vic map
        /* context write */
        context.write(new Text(myVIN), new AvroValue<VinImpressionCounts>(myVIC.build()));
    }
}
