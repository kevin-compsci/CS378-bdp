/* 
Kevin Nguyen
kdn433
CS378 - Big Data Programming - Project 6
*/

package com.refactorlabs.cs378.assign5;

import com.refactorlabs.cs378.sessions.*;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.*;

/**
 * Map class
 */
public class UserSessionMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, Text, AvroValue<VinImpressionCounts>> {
    /* map class */
    public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context) throws IOException, InterruptedException {
        /* Local declarations */
        Boolean first = true;
        String myVIN = "";
        List<Event> myEList = value.datum().getEvents();
        //HashMap<String, VinImpressionCounts> master = new HashMap<String, VinImpressionCounts>();
        Map<CharSequence, Long> subtypeMap = new HashMap<CharSequence, Long>();
        VinImpressionCounts.Builder myVIC = VinImpressionCounts.newBuilder(); //build VIC object
        /* iterate over all events */
        for (Event myEvent : myEList) {
            if(first) {
                myVIN = myEvent.getVin().toString(); //VINs will be the same for every event per user
                first = false;
            }
            //set count for Unique users 
            myVIC.setUniqueUsers(1L);
            //subtype
            String currentSubType = myEvent.getEventSubtype().toString();
            /* set map data for CLICKS */
            if (myEvent.getEventType().toString().toUpperCase().contains("CLICK") && subtypeMap.get(currentSubType) == null) { //doesn't exist
                subtypeMap.put(currentSubType, 1L); //put into map
            }
            myVIC.setClicks(subtypeMap); //set vic map
            /* set edit contact form data */
            if (myEvent.getEventType().toString().toUpperCase().equals("EDIT") || myEvent.getEventSubtype().toString().toUpperCase().equals("CONTACT_FORM")) {
                myVIC.setEditContactForm(1L);
            }
        }
        /* context write */
        context.write(new Text(myVIN), new AvroValue<VinImpressionCounts>(myVIC.build()));
    }
}
