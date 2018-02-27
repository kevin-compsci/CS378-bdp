/* 
Kevin Nguyen
kdn433
CS378 - Big Data Programming - Project 10
*/

package com.refactorlabs.cs378.assign10;

import java.io.*;
import java.util.*;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class BookKey implements Comparator<String>, Serializable {
	/* constructor */
	public BookKey() {
	}
	/* compare new data attached to key; data is size of list */
	@Override
	public int compare(String t1, String t2) {
		int size1 = Integer.parseInt(t1.split("::")[1]);
		int size2 = Integer.parseInt(t2.split("::")[1]);	
		if(size1 < size2) {
			return 1;
		}
		else if(size1 > size2) {
			return -1;
		}
	 	return 0;
	}
}	