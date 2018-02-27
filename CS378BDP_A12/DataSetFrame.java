/*
Kevin Nguyen
UTEID: kdn433

Big Data Programming - Assignment 12

*/

/*
Query 1 output:
 
Acura, TL, 5747.0, 25900.0, 15159.25
Acura,TSX,8900.0,8900.0,8900.0
Audi,A4,4500.0,12500.0,8665.0
 
Query 2 output:
 
2000,73112,233912,158495.44444444444
2001,51035,262983,165276.64285714287
2002,47070,213483,139307.03846153847
 
Query 3 output:
 
19UUA765X8A049739,display,1
19UUA765X8A049739,show,3
19UUA765X8A049739,visit,2

*/

package com.refactorlabs.cs378.assign12;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import java.util.*;
import java.io.*;

/**
 */
public class DataSetFrame {
	public static void main(String[] args) {
		//Utils.printClassPath();

		String inputFilename = args[0];
		//String outputFilename = args[1];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(DataSetFrame.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
            SparkSession session = SparkSession.builder().config(conf).getOrCreate();
		try {
        
                  //dataset
                  Dataset<Row> test = session.read().format("csv").option("header", "true").load(inputFilename);

                  //table view
                  test.registerTempTable("table1");

                  //sql queries
                  Dataset<Row> t = session.sql("select make, model, MIN(CAST(price AS int)), MAX(CAST(price AS int)), AVG(distinct CAST(price AS double)) from table1 where price > 0 group by make, model order by make, model");
                  Dataset<Row> v = session.sql("select year, MIN(CAST(mileage as int)), MAX(CAST(mileage AS int)), AVG(distinct CAST(mileage as double)) from table1 where mileage > 0 group by year order by year");
                  Dataset<Row> n = session.sql("select vin, substring_index(event, ' ', 1), COUNT(substring_index(event, ' ', 1)) from table1 group by vin, substring_index(event, ' ', 1) order by vin, substring_index(event, ' ', 1)");

                  //output sql queries
                  t.repartition(1).write().format("csv").save("Part1_TEST16");
                  v.repartition(1).write().format("csv").save("Part2_TEST16");
                  n.repartition(1).write().format("csv").save("Part3_TEST16");

            } 
            finally {
                  // Shut down the context
                  sc.stop();
            }
	}
}