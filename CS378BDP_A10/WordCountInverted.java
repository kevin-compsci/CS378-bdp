/* 
Kevin Nguyen
kdn433
CS378 - Big Data Programming - Project 10
*/

package com.refactorlabs.cs378.assign10;

import com.refactorlabs.cs378.assign10.BookVerse2;
import com.refactorlabs.cs378.assign10.BookKey;
import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.*;
import java.io.*;

/**
 * WordCount application for Spark.
 */
public class WordCountInverted {
	public static void main(String[] args) {
		//Utils.printClassPath();

		String inputFilename = args[0]; //take in a text file
		String outputFilename = args[1]; //output a file

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(WordCountInverted.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		try {
            // Load the input data
            JavaRDD<String> input = sc.textFile(inputFilename);

            // Split the input into words
            FlatMapFunction<String, String> splitFunction = new FlatMapFunction<String, String>() {
                @Override
                public Iterator<String> call(String line) throws Exception {
                    Boolean first = true;
                    String id = "", token = "", idWithToken = "";
                    /* PARSE STRING HERE, RETURN A WORDLIST ITERATOR */
                    line = line.toLowerCase(); //all lowercase
                    
                    StringTokenizer tokenizer = new StringTokenizer(line); //each word in line; IDtoken --> seqTokens
                    List<String> wordList = Lists.newArrayList();
                    // For each word in the input line, emit that word.
                    while (tokenizer.hasMoreTokens()) {
                        if(first) {
                            id = tokenizer.nextToken(); //get id token
                            first = false;
                            continue;
                        }
                        token = tokenizer.nextToken(); //get token (seqToken...not IDtoken)

                        /* remove punctuation */
                        token = token.replace(",","");
                        token = token.replace(".","");
                        token = token.replace("]","");
                        token = token.replace("[","");
                        token = token.replace("?","");
                        token = token.replace("!","");
                        token = token.replace("@","");
                        token = token.replace("$","");
                        token = token.replace("%","");
                        token = token.replace("^","");
                        token = token.replace("*","");
                        token = token.replace("&","");
                        token = token.replace(":","");
                        token = token.replace(";","");
                        token = token.replace("(","");
                        token = token.replace(")","");

                        idWithToken = id + "_" + token; //id;token --> ; separator
                        wordList.add(idWithToken); //add to list
                    }
                    return wordList.iterator(); //iterator should now have an iterable object of id;token strings
                }
            };

            // Transform into word and count
            PairFunction<String, String, List<String>> addCountFunction = new PairFunction<String, String, List<String>>() {
                @Override
                public Tuple2<String, List<String>> call(String s) throws Exception {
                    String[] piecesOfData = s.split("_"); //split by ; delimiter
                    String dataID = piecesOfData[0], dataToken = piecesOfData[1]; //dataID = id and dataToken = token
                    List<String> listID = new ArrayList<String>(); //form list
                    listID.add(dataID); //add element into list
                    return new Tuple2(dataToken, listID); //return as (Key = token, Value = list[id])
                }
            };

            // Sum the counts
            Function2<List<String>, List<String>, List<String>> sumFunction = new Function2<List<String>, List<String>, List<String>>() {
                @Override
                public List<String> call(List<String> id1, List<String> id2) throws Exception {
                    for(String v : id2) {
                        id1.add(v); //add id2 elements to id1?
                    }
                    Collections.sort(id1);
                    /* merge into hashmap */
                    HashSet<String> dataSet = new HashSet<String>();
                    for(String w : id1) {
                        dataSet.add(w);
                    }
                    /* turn into book object for comparison */
                    List<BookVerse2> unsortedList = new ArrayList<BookVerse2>();
                    for(String r : dataSet) {
                        unsortedList.add(new BookVerse2(r));
                    }
                    Collections.sort(unsortedList);
                    /* turn into strings */
                    List<String> sortedList = new ArrayList<String>();
                    for(BookVerse2 f : unsortedList) {
                        sortedList.add(f.toString());
                    }
                    return sortedList;
                }
            };
            /* algorithm main is here */
            JavaRDD<String> words = input.flatMap(splitFunction);
            JavaPairRDD<String, List<String>> wordsWithCount = words.mapToPair(addCountFunction);
            JavaPairRDD<String, List<String>> counts = wordsWithCount.reduceByKey(sumFunction);
            JavaPairRDD<String, List<String>> ordered = counts.sortByKey();

            /* EC Algorithm below */
            Map<String, List<String>> map = ordered.collectAsMap();
            HashMap<String, List<String>> hmap = new HashMap<String, List<String>>(map);
            HashMap<String, List<String>> newHMap = new HashMap<String, List<String>>();
            List<Tuple2> test = new ArrayList<Tuple2>();
            /* hash map conversions to edit keys while maintaining original values */
            for(Map.Entry<String, List<String>> entry : hmap.entrySet()) {
                //System.out.println(entry.getKey() + " : " + entry.getValue());
                newHMap.put(entry.getKey()+"::"+Integer.toString(entry.getValue().size()), entry.getValue());
            }
            for(Map.Entry<String, List<String>> entry : newHMap.entrySet()) {
                //System.out.println(entry.getKey() + " : " + entry.getValue());
                test.add(new Tuple2(entry.getKey(), entry.getValue()));
            }
            /* convert list into JavaPairRDD then sort it */
            JavaRDD newRDD = sc.parallelize(test);
            JavaPairRDD<String, List<String>> newPRDD = JavaPairRDD.fromJavaRDD(newRDD);
            JavaPairRDD<String, List<String>> newPRDD2 = newPRDD.sortByKey();
            JavaPairRDD<String, List<String>> ecOrdered = newPRDD2.sortByKey(new BookKey());
            /* END of EC Algorithm */

            // Save the word count to a text file (initiates evaluation)
            ordered.saveAsTextFile(outputFilename);
            ecOrdered.saveAsTextFile(outputFilename+"::ec");
        } finally {
            // Shut down the context
            sc.stop();
        }
	}

}
