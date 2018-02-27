/* 
Kevin Nguyen
kdn433
CS378 - Big Data Programming - Project 11
Original code given by Dr. Franke, CS378 Big Data Programming
*/

package com.refactorlabs.cs378.assign11;

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

public class Event implements Serializable {  
    String eventType; 
    String eventSubType; 
    String eventTimestamp; 
    String vin;  
    public String toString() { 
        return "<" + eventType + ":" + eventSubType + "," + eventTimestamp + "," + vin + ">";
    }  
}