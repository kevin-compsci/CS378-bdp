/* 
Kevin Nguyen
kdn433
CS378 - Big Data Programming - Project 11
*/

package com.refactorlabs.cs378.assign11;

import com.refactorlabs.cs378.assign11.Event;
import com.refactorlabs.cs378.assign11.CustomPartitioner;
import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkContext;
import org.apache.spark.util.*;
import org.apache.spark.Accumulator;
import scala.Tuple2;
import java.util.*;
import java.io.*;

/**
 * WordCount application for Spark.
 */
public class EventSessions {
	public static void main(String[] args) {
		String inputFilenameA = args[0];
        String inputFilenameB = args[1];
		String outputFilename = args[2];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(EventSessions.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
        SparkContext sparkCon = sc.sc();
        LongAccumulator eventCountPreFilter = sparkCon.longAccumulator("Event Count (prefilter)");
        LongAccumulator eventCountPostFilter = sparkCon.longAccumulator("Event Count (postfilter)");
        LongAccumulator sessCount = sparkCon.longAccumulator("Session Count");
        LongAccumulator sessCountShower = sparkCon.longAccumulator("Session Count Shower");
        LongAccumulator sessCountShowerFiltered = sparkCon.longAccumulator("Session Count filtered Shower");

		try {
            // Load the input data
            JavaRDD<String> input = sc.textFile(inputFilenameA);//+","+inputFilenameB);
            JavaRDD<String> input2 = sc.textFile(inputFilenameB);

            // accumulator intiialize here
            //Accumulator<Integer> showerSessionCount = sc.accumulator(0);

            // Split the input into words
            FlatMapFunction<String, String> splitFunction = new FlatMapFunction<String, String>() {
                public Iterator<String> call(String line) throws Exception {
                    /* Local declarations */
                    StringTokenizer tokenizer = new StringTokenizer(line);
                    List<String> wordList = Lists.newArrayList();
                    String token = "", temp = "", result = "";
                    boolean stopAdding = false;
                    int counter = 0;
                    // For each word in the input line, emit that word.
                    while (tokenizer.hasMoreTokens()) {
                        token = tokenizer.nextToken();
                        counter++;
                        /* checks id and resets counter */
                        if(token.length() == 9 && !token.contains(".") && !token.contains(":") && (token.charAt(0) > 47 && token.charAt(0) < 58)) {
                            try {
                                Integer.parseInt(token);
                            }
                            catch(NumberFormatException e) {
                                counter = 1;
                                stopAdding = false;
                            }
                        }
                        /* append data to result string */
                        if(!stopAdding) {
                            result += "_"+token;
                        }
                        /* signals on next iteration to stop appending until new ID is found */
                        if(token.length() == 17) {
                            stopAdding = true;
                        }   
                    }
                    result = result.substring(1, result.length()); //remove extra "_"
                    wordList.add(result); //add wordlist to result
                    return wordList.iterator(); //return as iterator type
                }
            };

            // Transform into word and count
            PairFunction<String, Tuple2<String, String>, List<Event>> addCountFunction = new PairFunction<String, Tuple2<String, String>, List<Event>>() {
                public Tuple2<Tuple2<String, String>, List<Event>> call(String s) throws Exception {
                    /* Local declarations */
                    String[] data = s.split("_");
                    List<Event> eventList = new ArrayList<Event>();
                    Event myEvent = new Event();
                    int count = 1;
                    String id = "", city = "", eType = "", eSubtype = "", eTimestamp = "", eVin = "";
                    String temp = "", temp2 = "", temp3 = "", temp4 = "";
                    Boolean isTwo = false, isThree = false;
                    /* Iterate through every data */
                    for(String token : data) {
                        /* based on counter, record info into variables that will be stored in Tuple or List respectively */
                        if(count == 1) { //id
                            id = token;
                        }
                        else if(count == 2) { //etype
                            eType = token;
                        }
                        else if(count == 3) { //eSubtype
                            eSubtype = token;
                            /* pre-checks for possible two/three part words */
                            if(token.equals("badge") || token.equals("vehicle") || token.equals("photo") || token.equals("get") || token.equals("contact") || token.equals("market")) {
                                count = 3;
                                temp = token;
                                isTwo = true;
                                continue;
                            }
                            /* the current token does have an incoming secondary word */
                            if(isTwo) {
                                eSubtype = temp+" "+token;
                                isTwo = false;
                            }
                        }
                        else if(count == 4) { // time
                            eTimestamp = token;
                            /* pre-checks for possible two/three part words */
                            if(token.contains("-")) {
                                count = 4;
                                temp2 = token;
                                isTwo = true;
                                continue;
                            }
                            /* the current token does have an incoming secondary word */
                            if(token.contains(":") && isTwo) {
                                eTimestamp = temp2+" "+token;
                                isTwo = false;
                            }
                        }
                        else if(count == 5) { //city
                            city = token;
                            /* pre-checks for possible two/three part words */
                            if(token.equals("Chapel") || token.equals("Mt.") || token.equals("New") || token.equals("North") || token.equals("St.") || token.equals("Stone")) {
                                count = 5;
                                temp3 = token;
                                isTwo = true;
                                continue;
                            }
                            else if((token.equals("Myrtle") && temp3.equals("North")) || (token.equals("Port") && temp3.equals("New"))) {
                                count = 5;
                                temp4 = temp3+" "+token;
                                isThree = true;
                                isTwo = false;
                                continue;
                            }

                            /* the current token does have an incoming secondary word */
                            if(isTwo) {
                                city = temp3+" "+token;
                                isTwo = false;
                            }
                            /* the current token does have an incoming third word */
                            if(isThree) {
                                city = temp4+" "+token;
                                isThree = false;
                            }
                        }
                        else if(count == 6) { //vin
                            eVin = token;
                        }
                        count++;
                    }
                    /* set data to event object */
                    myEvent.eventType = eType;
                    myEvent.eventSubType = eSubtype;
                    myEvent.eventTimestamp = eTimestamp;
                    myEvent.vin = eVin;
                    /* add event object to event list */
                    eventList.add(myEvent);
                    return new Tuple2(new Tuple2(id, city), eventList); //return complete unsorted data
                }
            };

            // merge lists
            Function2<List<Event>, List<Event>, List<Event>> sumFunction = new Function2<List<Event>, List<Event>, List<Event>>() {
                public List<Event> call(List<Event> l1, List<Event> l2) throws Exception {
                    /* add everything to l1 list */
                    for(Event e : l2) {
                        l1.add(e);
                    }

                    /* merge into hashmap */
                    HashSet<Event> dataSet = new HashSet<Event>();
                    for(Event w : l1) {
                        dataSet.add(w);
                    }

                    /* turn into event object for comparison */
                    List<SortEvent> unsortedList = new ArrayList<SortEvent>();
                    for(Event r : dataSet) {
                        unsortedList.add(new SortEvent(r));
                    }
                    Collections.sort(unsortedList);

                    /* return as list */
                    List<Event> sortedList = new ArrayList<Event>();
                    for(SortEvent f : unsortedList) {
                        sortedList.add(f.getEvent());
                    }
                    return sortedList;
                }
            };


            Function<Tuple2<Tuple2<String, String>, List<Event>>, Boolean> filterFunction = new Function<Tuple2<Tuple2<String, String>, List<Event>>, Boolean>() {
                Random rand = new Random();
                public Boolean call(Tuple2<Tuple2<String, String>, List<Event>> data) {
                    Boolean result = true;
                    Double randValue = 0.0;
                    String eventType = "";
                    String[] submitterCheck = {"edit", "change", "submit"};
                    String[] clickerCheck = {"click"};
                    String[] showerCheck = {"show", "display"};
                    String[] visitorCheck = {"visit"};
                    List submitterCheckList = Arrays.asList(submitterCheck); //list of event type checking
                    List clickerCheckList = Arrays.asList(clickerCheck); //list of event type checking
                    List showerCheckList = Arrays.asList(showerCheck); //list of event type checking
                    List visitorCheckList = Arrays.asList(visitorCheck); //list of event type checking
                    Boolean isSubmit = false, isClick = false, isShow = false, isContactForm = false, isFilterable = true;
                    /* iterate and filter SHOWER SESSIONS */
                    for(Event myEvent : data._2()) {
                        eventType = myEvent.eventType; //get event type
                        /* If statements to categorize sessions */
                        if(submitterCheckList.contains(eventType) || clickerCheckList.contains(eventType) || (myEvent.eventSubType.equals("contact form") && !eventType.equals("visit"))) {
                           isFilterable = false;
                           break;
                        }
                        /* is shower? */
                        if(showerCheckList.contains(eventType)) {
                            isShow = true;
                        }
                    }
                    /* if statements to check which session type it is */
                    if(isShow && isFilterable) {
                        sessCountShower.add(1);
                        if (rand.nextDouble() > 0.10) {
                            sessCountShowerFiltered.add(1);
                            result = false;
                        }
                    }
                    return result;
                }
            };

            /* Logic begins here on JavaRDD's and JavaPairRDD's */
            JavaRDD<String> words1 = input.flatMap(splitFunction);
            JavaRDD<String> words2 = input2.flatMap(splitFunction);
            JavaRDD<String> words = words1.union(words2);
            JavaPairRDD<Tuple2<String, String>, List<Event>> wordsWithCount = words.mapToPair(addCountFunction);
            JavaPairRDD<Tuple2<String, String>, List<Event>> counts = wordsWithCount.reduceByKey(sumFunction);
            JavaPairRDD<Tuple2<String, String>, List<Event>> ordered = counts.sortByKey(new SortSession(), true);
            JavaPairRDD<Tuple2<String, String>, List<Event>> filterOrdered = ordered.filter(filterFunction);
            /* accumulate prefilter info */
            for (Tuple2<Tuple2<String, String>, List<Event>> t : ordered.collect()) {
                sessCount.add(1);
                for(Event myEvent : t._2()) {
                    eventCountPreFilter.add(1);
                }
            }
            /* accumulate postfilter info */
            for (Tuple2<Tuple2<String, String>, List<Event>> t : filterOrdered.collect()) {
                for(Event myEvent : t._2()) {
                    eventCountPostFilter.add(1);
                }
            }
            /* output accumulator values */
            System.out.println(" \n Event Count (prefilter): " + eventCountPreFilter.value() + "\n");
            System.out.println(" \n Event Count (postfilter): " + eventCountPostFilter.value() + "\n");
            System.out.println(" \n Session Count: " + sessCount.value() + "\n");
            System.out.println(" \n Session Count Shower: " + sessCountShower.value() + "\n");
            System.out.println(" \n Session Count Shower Filtered: " + sessCountShowerFiltered.value() + "\n");
            /* Logic ends here on JavaRDD's and JavaPairRDD's */
            JavaPairRDD<Tuple2<String, String>, List<Event>> pOrdered = filterOrdered.partitionBy(new CustomPartitioner(6));
            // Save the word count to a text file (initiates evaluation)
            pOrdered.saveAsTextFile(outputFilename);
        } 
        finally {
            // Shut down the context
            sc.stop();
        }
	}
}