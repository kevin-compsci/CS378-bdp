/* 
Kevin Nguyen
kdn433
CS378 - Big Data Programming - Project 11
*/

package com.refactorlabs.cs378.assign11;

import java.io.*;
import java.util.*;
import com.refactorlabs.cs378.assign11.Event;

public class SortEvent implements Comparable<SortEvent>, Serializable {

	Event myEvent;
	String timestamp;

	/* constructor */
	public SortEvent() {
	}

	/* constructor with params */
	public SortEvent(Event e) {
		this.myEvent = e;
		this.timestamp = this.myEvent.eventTimestamp;
	}

	/* returns the event */
	public Event getEvent() {
		return myEvent;
	}

	/* compareTo override */
	public int compareTo(SortEvent e) {
		int result = timestamp.compareTo(e.myEvent.eventTimestamp);
		/* if not equal then return value otherse return 0 */
		if(result != 0) {
			return result;
		}
		else {
			return 0;
		}		
	}

	/* compare override */
	public int compare(Event e1, Event e2) {
		int event1 = Integer.parseInt(e1.eventTimestamp);
		int event2 = Integer.parseInt(e2.eventTimestamp);
		/* if not equal then return value otherse return 0 */
		if(event1 < event2) {
			return -1;
		}
		else if(event1 > event2) {
			return 1;
		}
		return 0;
	}
}	
