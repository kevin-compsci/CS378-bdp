/* 
Kevin Nguyen
kdn433
CS378 - Big Data Programming - Project 11
*/

package com.refactorlabs.cs378.assign11;

import java.io.*;
import java.util.*;
import com.refactorlabs.cs378.assign11.Event;
import scala.Tuple2;
import org.apache.spark.Partitioner;
import org.apache.spark.HashPartitioner;



public class CustomPartitioner extends HashPartitioner {
	int numOfBuckets;

	/* constructor */	
	public CustomPartitioner(int value) {
		super(value);
		this.numOfBuckets = value;
	}
 
 	/* returns number of partitions */
	public int numPartitions() {
	    return this.numOfBuckets;
	}
 
 	/* returns the partition value */
	public int getPartition(Object key) {
		return Math.abs(((Tuple2<String, String>) key)._2().hashCode() % numOfBuckets);
	}
	
	/* check if object is equal to this */
	public boolean equals(Object obj) {
		CustomPartitioner tempObj = (CustomPartitioner) obj;
		/* check for falsehood */
		if((obj instanceof CustomPartitioner) && (tempObj.numPartitions() == this.numPartitions())) {
			return true;
		}
		return false;
	}
}	
