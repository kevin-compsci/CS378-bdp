/* 
Kevin Nguyen
kdn433
CS378 - Big Data Programming - Project 10
*/

package com.refactorlabs.cs378.assign10;

import java.io.*;
import java.util.*;

public class BookVerse2 implements Comparable<BookVerse2> {

	/* Declarations */
	String prefix; //id
	String chapter; //chapter #
	String section; //section #

	public BookVerse2() {
	}
	/* constructor that takes input; split on ":" and store data */
	public BookVerse2(String book) {
		String[] x = book.split(":");
		this.prefix = x[0];
		this.chapter = x[1];
		this.section = x[2];
	}
	/* get prefix (book name) */
	public String getPrefix() {
		return prefix;
	}
	/* get chapter */
	public String getChapter() {
		return chapter;
	}
	/* get section (verse) */
	public String getSection() {
		return section;
	}
	/* set prefix (book name) */
	public void setPrefix(String id) {
		this.prefix = id;
	}
	/* set chapter */
	public void setChapter(String chap) {
		this.chapter = chap;
	}
	/* set section (verse) */
	public void setSection(String sect) {
		this.section = sect;
	}

	/* tostring handler */
	public String toString() {
		return prefix + ":" + chapter + ":" + section;
	}

	/* compareTo override */
	public int compareTo(BookVerse2 b) {
		/* if cases */
		if(!this.getPrefix().equals(b.getPrefix())) {
			if(this.getPrefix().charAt(0) > b.getPrefix().charAt(0)) {
				return 1;
			}
			else if(this.getPrefix().charAt(0) < b.getPrefix().charAt(0)) {
				return -1;
			}
			else {
				if(this.getPrefix().charAt(1) > b.getPrefix().charAt(1)) {
					return 1;
				}
				else if(this.getPrefix().charAt(1) < b.getPrefix().charAt(1)) {
					return -1;
				}	
			}
			return 0;
		}
		else if(this.getPrefix().equals(b.getPrefix()) && !this.getChapter().equals(b.getChapter())) {
			if(Integer.parseInt(this.getChapter()) > Integer.parseInt(b.getChapter())) {
				return 1;
			}
			else if(Integer.parseInt(this.getChapter()) < Integer.parseInt(b.getChapter())) {
				return -1;
			}
			return 0;
		}
		else {
			if(Integer.parseInt(this.getSection()) > Integer.parseInt(b.getSection())) {
				return 1;
			}
			else if(Integer.parseInt(this.getSection()) < Integer.parseInt(b.getSection())) {
				return -1;
			}
			return 0;
		}
	}
	/* comparison */
	public int compare(BookVerse2 b1, BookVerse2 b2) {
		if(b1.getPrefix().equals(b2.getPrefix()) && !b1.getChapter().equals(b2.getChapter())) {
			return Integer.parseInt(b1.getChapter()) - Integer.parseInt(b2.getChapter());
		}
		else if(b1.getPrefix().equals(b2.getPrefix()) && b1.getChapter().equals(b2.getChapter())) {
			return Integer.parseInt(b1.getSection()) - Integer.parseInt(b2.getSection());
		}
		return b1.getPrefix().compareTo(b2.getPrefix());
	}
}	
