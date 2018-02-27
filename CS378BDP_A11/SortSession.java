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


//HOW TO SORT TUPLE? --> KEYS ---> STRING = ID,CITY

public class SortSession implements Comparator<Tuple2<String, String>>, Serializable {
	/* constructor */
	public SortSession() {
	}

	/* compare override */
	public int compare(Tuple2<String, String> s1, Tuple2<String, String> s2) {
		String idS1 = s1._1(); //id for first Tuple
		String cityS1 = s1._2(); //city for first Tuple
		String idS2 = s2._1(); //id for second Tuple
		String cityS2 = s2._2(); //city for second Tuple

		int result = idS1.compareTo(idS2);
		int result2 = cityS1.compareTo(cityS2);

		if(result != 0) { //not equal then just return comparison
			return result;
		}
		else {
			return result2;
		}
	}
}	
