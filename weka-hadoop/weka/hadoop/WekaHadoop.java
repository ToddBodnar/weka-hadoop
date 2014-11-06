/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author toddbodnar
 */
public class WekaHadoop {
    public static void main(String args[]) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Weka-Hadoop");
        job.setJarByClass(WekaHadoop.class);
        job.setMapperClass(WekaMapper.class);
        job.setCombinerClass(WekaReduce.class);
        job.setReducerClass(WekaReduce.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setInputFormatClass(WekaExperimentInput.class);
        //job.setOutputFormatClass(WekaOutput.class);
        
        WekaExperimentInput.setInfile(job, new File(args[0]));
        
        FileOutputFormat.setOutputPath(job, new Path("/weka_results123"));
        
        utils.LOG.info("c1");
        
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
