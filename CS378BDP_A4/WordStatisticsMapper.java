/*
Kevin Nguyen
EID: kdn433
CS378 Big Data Programming
*/

package com.refactorlabs.cs378.assign4;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.*;

/**
 * Map class for word statistics using avro
 */
public class WordStatisticsMapper extends Mapper<LongWritable, Text, Text, AvroValue<WordStatisticsData>> {
    /* Local private declarations */
    private Text word = new Text();
    /* Mapper function */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /* get line and tokens per line */
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        HashMap<String, Double> countStorage = new HashMap<String, Double>(); //hashmap to store counts of all words
        /* Go through each token */
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken(); //get token from tokenizer
            token = token.toLowerCase(); //lowercase all characters in token
            /* put word into hashmap or not, and adjust value accordingly */
            if (countStorage.get(token) != null) {
                countStorage.put(token,countStorage.get(token)+1.0); //store word and increment its count
            }
            else {
                countStorage.put(token, 1.0); //put word into map with initial count
            }
        }
        /* gO through every item in hashmap */
        //builder in loop, 1 per set of data ---> word: set(count,count_squared,count_tracker,mean,variance)
        //context write data
        for (Map.Entry<String, Double> entry : countStorage.entrySet()) {
            String mapKey = entry.getKey(); //get key for iterative instance in map
            Double mapValue = entry.getValue(); //get value iterative instance in map
            Double count_squared = mapValue * mapValue;
            /* build and store data into builder, then context write */
            WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
            builder.setTotalCount(mapValue); //<--- set data count
            builder.setSumOfSquares(count_squared); // <--- set data squared
            builder.setDocumentCount(1.0); //<--- set # of doc occurances
            builder.setMin(1.0); // <---- min value of occurances in doc 
            builder.setMax(1.0); // <---- max value of occurances in doc
            builder.setMean(0.0); // <---- set min value
            builder.setVariance(0.0); // <---- set max value
            word.set(mapKey); //place key into word
            context.write(word, new AvroValue<WordStatisticsData>(builder.build())); //write context
        }
    }
}