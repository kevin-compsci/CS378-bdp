/* 
Kevin Nguyen
kdn433
CS378 - Big Data Programming - Project 7
*/

package com.refactorlabs.cs378.assign6;

import com.refactorlabs.cs378.utils.Utils;
import com.refactorlabs.cs378.sessions.*;
import com.refactorlabs.cs378.assign6.SessionType;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.*;

/**
 * Map class
 */
public class UserSessionMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, Text, Text> {

	private AvroMultipleOutputs mo;

    Random rand = new Random(); //random class
    /* map class */
    public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context) throws IOException, InterruptedException {
    
    	// /* Local declarations */
    	String eventType = null, eventSubtype = null;
    	String[] submitterCheck = {"EDIT", "CHANGE", "SUBMIT", "CONTACT_FORM"};
    	String[] clickerCheck = {"CLICK"};
    	String[] showerCheck = {"SHOW", "DISPLAY"};
    	String[] visitorCheck = {"VISIT"};
    	List submitterCheckList = Arrays.asList(submitterCheck); //list of event type checking
    	List clickerCheckList = Arrays.asList(clickerCheck); //list of event type checking
    	List showerCheckList = Arrays.asList(showerCheck); //list of event type checking
    	List visitorCheckList = Arrays.asList(visitorCheck); //list of event type checking
		String sessCat = SessionType.SUBMITTER.getText(); //session category
		String clickCat = SessionType.CLICKER.getText(); //clicker category
		String showerCat = SessionType.SHOWER.getText(); //shower category
		String visCat = SessionType.VISITOR.getText(); //visitor category
		String otherCat = SessionType.OTHER.getText(); //other category
		Boolean isSubmit = false, isClick = false, isShow = false, isVisit = false, isOther = false;


    	List<Event> myEList = value.datum().getEvents(); //get list of events
    	/* Loop through every event */
    	//myEList.size() check size and reject and increment counter
    	if (myEList.size() < 100) {
	    	for (Event myEvent : myEList) {
		    	eventType = myEvent.getEventType().toString(); //get event type
		    	eventSubtype = myEvent.getEventSubtype().toString(); //get event subtype
		    	/* If statements to categorize sessions */
		    	if(submitterCheckList.contains(eventType) && submitterCheckList.contains(eventSubtype)) {
		    		//DO STUFF - submitter category
		    		isSubmit = true;
		    	}
		    	else if(clickerCheckList.contains(eventType) && !submitterCheckList.contains(eventType)) {
		    		//DO STUFF - clicker category
		    		isClick = true;
		    	}
		    	else if(showerCheckList.contains(eventType) && !clickerCheckList.contains(eventType)) {
		    		//DO STUFF - shower category
		    		isShow = true;
		    	}
		    	else if(visitorCheckList.contains(eventType) && !showerCheckList.contains(eventType)) {
		    		//DO STUFF - visitor category
		    		isVisit = true;
		    	}
		    	else{
		    		//DO STUFF - other category
		    		isOther = true;
		    	}
	    	} //end loop
	    }
    	else {
    		context.getCounter(Utils.LARGE_EVENTS, "Event Size Over 100").increment(1L);
    	}

    	/* if statements to check which session type it is */
    	if(isSubmit) {
    		mo.write(sessCat, key, value);
    	}
    	else if(isClick && !isSubmit) {
    		if (rand.nextDouble() < 0.1) {
    			mo.write(clickCat, key, value);
    		}
    		else {
    			context.getCounter(Utils.CLICK_SAMPLE, "Clicker Sampling Rejected").increment(1L);
    		}
    	}
    	else if (isShow && !isClick) {
    		if (rand.nextDouble() < 0.5) {
    			mo.write(showerCat, key, value);
    		}
    		else {
    			context.getCounter(Utils.SHOW_SAMPLE, "Shower Sampling Rejected").increment(1L);
    		}
    	}
    	else if (isVisit && !isShow) {
    		mo.write(visCat, key, value);
    	}
    	else {
    		mo.write(otherCat, key, value);
    	}
    }

    public void setup(Context context) {
    	mo = new AvroMultipleOutputs(context);
    }

    public void cleanup(Context context) throws IOException {
    	try {
    		mo.close();
    	}
    	catch (InterruptedException e) {
    		System.out.println("ERROR");
    	}
    }
}