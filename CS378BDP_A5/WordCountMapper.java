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
 * Map class for various WordCount examples that use the AVRO generated class WordCountData.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, AvroValue<WordCountData>> {

    /**
     * Local variable "word" will contain the word identified in the input.
     */
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        int count = 0;
        CharSequence token = "";
        context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

        ArrayList<CharSequence> eventAttributes = new ArrayList<CharSequence>(); 
        CharSequence tempArray[] = line.split("\\t");
        if(tempArray != null && tempArray.length > 0) {
            for(CharSequence myValue : tempArray) {
                eventAttributes.add(myValue); //add every element to array
            }
        }
        /* loop through every item in array and determine where they belong */
        while (count < eventAttributes.size()) {
            WordCountData.Builder builder = WordCountData.newBuilder();
            token = eventAttributes.get(count);
            /* check empty fields */
            if (token.toString().isEmpty()) {
                count++;
                continue;
            }
            /* parse each field */
            if (count == 0) {
                word.set("User-Id:"+token.toString());
                builder.setCount(1);
                context.write(word, new AvroValue<WordCountData>(builder.build()));
            }
            else if (count == 1) {
                String temp = token.toString();
                String[] temp2A = temp.split("\\s");
                int count2 = 0, size = 0;
                for(String tempV : temp2A) {
                    size++;
                }
                while (count2 < size) {
                    if (count2 == 0) {
                        word.set("EventType:"+temp2A[count2]);
                        builder.setCount(1);
                        context.write(word, new AvroValue<WordCountData>(builder.build()));                        
                    }
                    else if (count2 == 1 && size == 2) {
                        word.set("EventSubtype:"+temp2A[count2]);
                        builder.setCount(1);
                        context.write(word, new AvroValue<WordCountData>(builder.build()));
                    }
                    else if (count2 == 1 && size == 3) {
                        word.set("EventSubtype:"+temp2A[count2]+" "+temp2A[count2+1]);
                        builder.setCount(1);
                        context.write(word, new AvroValue<WordCountData>(builder.build()));
                    }
                    count2++;
                }
            }
            else if (count == 2) {
                word.set("EventTime:"+token.toString());
                builder.setCount(1);
                context.write(word, new AvroValue<WordCountData>(builder.build()));
            }
            else if (count == 3) {
                word.set("City:"+token.toString());
                builder.setCount(1);
                context.write(word, new AvroValue<WordCountData>(builder.build()));
            }
            else if (count == 4) {
                word.set("VIN:"+token.toString());
                builder.setCount(1);
                context.write(word, new AvroValue<WordCountData>(builder.build()));
            }
            else if (count == 5) {
                word.set("Vechicle_Condition:"+token.toString());
                builder.setCount(1);
                context.write(word, new AvroValue<WordCountData>(builder.build()));
            }
            else if (count == 6) {
                word.set("Year:"+Integer.parseInt(token.toString()));
                builder.setCount(1);
                context.write(word, new AvroValue<WordCountData>(builder.build()));
            }
            else if (count == 7) {
                word.set("Make:"+token.toString());
                builder.setCount(1);
                context.write(word, new AvroValue<WordCountData>(builder.build()));
            }
            else if (count == 8) {
                word.set("Model:"+token.toString());
                builder.setCount(1);
                context.write(word, new AvroValue<WordCountData>(builder.build()));
            }
            else if (count == 9) {
                word.set("Trim:"+token.toString());
                builder.setCount(1);
                context.write(word, new AvroValue<WordCountData>(builder.build())); 
            }
            else if (count == 10) {
                word.set("Body_Style:"+token.toString());
                builder.setCount(1);
                context.write(word, new AvroValue<WordCountData>(builder.build()));
            }
            else if (count == 11) {
                word.set("Cab_Style:"+token.toString());
                builder.setCount(1);
                context.write(word, new AvroValue<WordCountData>(builder.build()));
            }
            else if (count == 12) {
                word.set("Price:"+Double.parseDouble(token.toString()));
                builder.setCount(1);
                context.write(word, new AvroValue<WordCountData>(builder.build()));
            }
            else if (count == 13) {
                word.set("Mileage:"+Integer.parseInt(token.toString()));
                builder.setCount(1);
                context.write(word, new AvroValue<WordCountData>(builder.build()));
            }
            else if (count == 14) {
                if (token.toString().equals("f")) {
                    word.set("Report: false");
                    builder.setCount(1);
                    context.write(word, new AvroValue<WordCountData>(builder.build()));
                }
                else if (token.toString().equals("t")) {
                    word.set("Report: true");
                    builder.setCount(1);
                    context.write(word, new AvroValue<WordCountData>(builder.build()));
                }
            }
            else if (count == 15) {
                /* features count */
                String featureLine = token.toString();
                CharSequence tempArray2[] = featureLine.split(":");
                ArrayList<CharSequence> eventAttributes2 = new ArrayList<CharSequence>(); 
                if(tempArray2 != null && tempArray2.length > 0) {
                    for(CharSequence myValue2 : tempArray2) {
                        eventAttributes2.add(myValue2); //add features to array
                    }
                }
                int count2 = 0;
                while (count2 < eventAttributes2.size()) {
                    WordCountData.Builder builder2 = WordCountData.newBuilder();
                    word.set(eventAttributes2.get(count2).toString());
                    builder2.setCount(1);
                    context.write(word, new AvroValue<WordCountData>(builder2.build()));
                    count2++;
                }
            }
            count++;
        }
    }
}
