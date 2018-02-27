/* 
Kevin Nguyen
EID: kdn433
CS378 Big Data Programming
*/

package com.refactorlabs.cs378.assign4;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.*;
import java.util.*;

/**
 * Word Statistics using AVRO
 */
public class WordStatisticsAvro extends Configured implements Tool {
    /* Reduce class */
    public static class ReduceClass extends Reducer<Text, AvroValue<WordStatisticsData>, Text, AvroValue<WordStatisticsData>> {
        /* local declarations */
        Double countTracker = 0.0, mean = 0.0, variance = 0.0, sum = 0.0, sum_count_squared = 0.0;
        Double currentMax = 1.0, currentMin = 1.0, temp = 1.0;
        /* Reduce method */
        public void reduce(Text key, Iterable<AvroValue<WordStatisticsData>> values, Context context) throws IOException, InterruptedException {
            /* loop through data in avro records */
            for (AvroValue<WordStatisticsData> value : values) {
                /* compute min */
                temp = value.datum().getTotalCount(); //get min for this value
                if (currentMin >= temp) {
                    currentMin = temp; //if old is smaller, then adjust
                }
                /* compute max */
                if (currentMax <= temp) {
                    currentMax = temp; //if old is bigger, then adjust
                }
                /* compute summation */
                sum += value.datum().getTotalCount(); //get sum for mean computation
                sum_count_squared += value.datum().getSumOfSquares(); //get sum of count_squared
                countTracker += value.datum().getDocumentCount(); //increments number of times word appears per doc
            }
            /* compute mean */
            mean = sum/countTracker;
            /* compute variance */
            variance = ((sum_count_squared/countTracker) - (mean * mean));
            /* use builder to set values to avro schema */
            WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
            builder.setTotalCount(sum); // <--- sum of counts
            builder.setSumOfSquares(sum_count_squared); // <---- sum of counts squared
            builder.setDocumentCount(countTracker); // <---- set # of doc occurances
            builder.setMin(currentMin); // <--- set min value (should always be atleast 1)
            builder.setMax(currentMax); // <---- set max value
            builder.setMean(mean); // <--- set mean value
            builder.setVariance(variance); // <--- set variance value
            context.write(key, new AvroValue<WordStatisticsData>(builder.build())); //context write
        }
    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordStatisticsAvro <input path> <output path>");
            return -1;
        }
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "WordStatisticsAvro");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordStatisticsAvro.class);
        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(WordStatisticsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, WordStatisticsData.getClassSchema());
        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        AvroJob.setOutputValueSchema(job, WordStatisticsData.getClassSchema());
        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPaths(job, appArgs[0]);
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));
        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);
        return 0;
    }

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        Utils.printClassPath();
        int res = ToolRunner.run(new WordStatisticsAvro(), args);
        System.exit(res);
    }
}