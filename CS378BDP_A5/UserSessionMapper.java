/* 
Kevin Nguyen
kdn433
CS378 - Big Data Programming - Project 5
*/

package com.refactorlabs.cs378.assign5;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.*;

/**
 * Map class
 */
public class UserSessionMapper extends Mapper<LongWritable, Text, Text, AvroValue<Session>> {
    /**
     * Local variable "word" will contain the word identified in the input.
     */
    private Text word = new Text();
    /* map class */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /* Local declarations */
        String line = value.toString();
        Session.Builder mySession = Session.newBuilder();
        Event.Builder myEvents = Event.newBuilder();
        int count = 0;
        CharSequence token = "";
        /* Parse line information here and get individual tokens of data */
        /* SET initial DATA HERE */
        /* EVENT ATTRIBUTES BELOW */
        ArrayList<CharSequence> eventAttributes = new ArrayList<CharSequence>(); 
        CharSequence tempArray[] = line.split("\\t");
        if(tempArray != null && tempArray.length > 0) {
            for(CharSequence myValue : tempArray) {
                eventAttributes.add(myValue); //add every element to array
            }
        }

        /* loop through every item in array and determine where they belong */
        while (count < eventAttributes.size()-1) {
            token = eventAttributes.get(count);
            if (token.toString().isEmpty()) {
                count++;
                continue;
            }
            if (count == 0) {
                String v = token.toString() + "_v_" + Integer.toString(count);
                mySession.setUserId(v);
                word.set(new Text(token.toString())); //set key as user-id?
            }
            else if (count == 1) {
                String temp = token.toString();
                String[] temp2A = temp.split("\\s");
                int count2 = 1, size = 0;
                for (String v : temp2A) {
                    if (count2 == 1) {
                        /* set event types */
                        if (line.contains("change")) {
                            myEvents.setEventType(EventType.CHANGE);
                        }
                        else if (line.contains("click")) {
                            myEvents.setEventType(EventType.CLICK);
                        }
                        else if (line.contains("display")) {
                            myEvents.setEventType(EventType.DISPLAY);
                        }
                        else if (line.contains("edit")) {
                            myEvents.setEventType(EventType.EDIT);
                        }
                        else if (line.contains("show")) {
                            myEvents.setEventType(EventType.SHOW);
                        }
                        else if (line.contains("submit")) {
                            myEvents.setEventType(EventType.SUBMIT);
                        }
                        else if (line.contains("visit")) {
                            myEvents.setEventType(EventType.VISIT);
                        }
                        count2++;
                    }
                    else if (count2 == 2) {
                        /* set event subtypes */
                        if (line.contains("badges")) {
                            myEvents.setEventSubtype(EventSubtype.BADGES);
                        }
                        else if (line.contains("alternative")) {
                            myEvents.setEventSubtype(EventSubtype.ALTERNATIVE);
                        }
                        else if (line.contains("contact form")) {
                            myEvents.setEventSubtype(EventSubtype.CONTACT_FORM);
                        }
                        else if (line.contains("market report")) {
                            myEvents.setEventSubtype(EventSubtype.MARKET_REPORT);
                        }
                        else if (line.contains("features")) {
                            myEvents.setEventSubtype(EventSubtype.FEATURES);
                        }
                        else if (line.contains("badge detail")) {
                            myEvents.setEventSubtype(EventSubtype.BADGE_DETAIL);
                        }
                        else if (line.contains("vehicle history")) {
                            myEvents.setEventSubtype(EventSubtype.VEHICLE_HISTORY);
                        }
                        else if (line.contains("get directions")) {
                            myEvents.setEventSubtype(EventSubtype.GET_DIRECTIONS);
                        }
                    }
                }
            }
            else if (count == 2) {
                myEvents.setEventTime(token);
            }
            else if (count == 3) {
                myEvents.setCity(token);
            }
            else if (count == 4) {
                myEvents.setVin(token);
            }
            else if (count == 5) {
                /* set event vehicle condition */
                if (line.contains("used")) {
                    myEvents.setVehicleCondition(VehicleCondition.USED);
                }
                else if (line.contains("new")) {
                    myEvents.setVehicleCondition(VehicleCondition.NEW);
                }
            }
            else if (count == 6) {
                myEvents.setYear(Integer.parseInt(token.toString()));
            }
            else if (count == 7) {
                myEvents.setMake(token);
            }
            else if (count == 8) {
                myEvents.setModel(token);
            }
            else if (count == 9) {
                myEvents.setTrim(token);
            }
            else if (count == 10) {
                /* set event body style */
                if (line.contains("suv")) {
                    myEvents.setBodyStyle(BodyStyle.SUV);
                }
                else if (line.contains("sedan")) {
                    myEvents.setBodyStyle(BodyStyle.SEDAN);
                }
                else if (line.contains("hatchback")) {
                    myEvents.setBodyStyle(BodyStyle.HATCHBACK);
                }
                else if (line.contains("pickup")) {
                    myEvents.setBodyStyle(BodyStyle.PICKUP);
                }
                else if (line.contains("coupe")) {
                    myEvents.setBodyStyle(BodyStyle.COUPE);
                }
                else if (line.contains("convertible")) {
                    myEvents.setBodyStyle(BodyStyle.CONVERTIBLE);
                }
            }
            else if (count == 11) {
                /* set event cab style */
                if (line.contains("extended cab")) {
                    myEvents.setCabStyle(CabStyle.EXTENDED_CAB);
                }
                else if (line.contains("crew cab")) {
                    myEvents.setCabStyle(CabStyle.CREW_CAB);
                }
                else if (line.contains("null")) {
                    myEvents.setCabStyle(CabStyle.NULL);
                }
            }
            else if (count == 12) {
                myEvents.setPrice(Double.parseDouble(token.toString()));
            }
            else if (count == 13) {
                myEvents.setMileage(Integer.parseInt(token.toString()));
            }
            else if (count == 14) {
                if (token.toString().equals("f")) {
                    myEvents.setFreeCarfaxReport(false);
                }
                else if (token.toString().equals("t")) {
                    myEvents.setFreeCarfaxReport(true);
                }
            }
            count++;
        } //end of loop
        count = 0;
        /* FEATURE BELOW */
        String features = eventAttributes.get(eventAttributes.size()-1).toString();
        CharSequence tempArray2[] = features.split(":");
        List<CharSequence> eventAttributes2 = new ArrayList<CharSequence>(); 
        if(tempArray2 != null && tempArray2.length > 0) {
            for(CharSequence myValue2 : tempArray2) {
                eventAttributes2.add(myValue2); //add features to array
            }
        }
        myEvents.setFeatures(eventAttributes2); //set features in events
        List<Event> myEList = new ArrayList<Event>();
        myEList.add(myEvents.build()); //build event in a new list
        mySession.setEvents(myEList); //set events in session with list of features
        /* context write */
        context.write(word, new AvroValue<Session>(mySession.build()));
    }
}
