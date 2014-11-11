/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author toddbodnar
 */
public class WekaHadoop {
    public static void main(String args[]) throws Exception
    {
        if(args.length != 2)
        {
            System.out.println("Error! Needs two arguements (input file + output location)");
            System.exit(-2);
        }
        
        //main based on PI example
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weka-Hadoop");
        job.setJarByClass(WekaHadoop.class);
        //job.setNumReduceTasks(3);
        job.setNumReduceTasks(1);

        
        //setup job conf
        job.setJobName("Weka-Hadoop");
        job.setJarByClass(WekaHadoop.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        //job.setOutputKeyClass(WekaJob.class);
        //job.setOutputValueClass(WekaFoldResults.class);
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WekaFoldResults.class);
        
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(WekaMapper.class);

        job.setReducerClass(WekaReduce.class);
        job.setNumReduceTasks(1);

        // turn off speculative execution, because DFS doesn't handle
        // multiple writers to the same file.
        job.setSpeculativeExecution(false);

        //setup input/output directories
        
        long now = System.currentTimeMillis();
        int rand = new Random().nextInt(Integer.MAX_VALUE);
        final Path tmpDir = new Path("WEKA_EXPERIMENT" + "_" + now + "_" + rand);

        final Path inDir = new Path(tmpDir, "in");
        final Path outDir = new Path(tmpDir, "out");
        FileInputFormat.setInputPaths(job, inDir);
        FileOutputFormat.setOutputPath(job, outDir);

        final FileSystem fs = FileSystem.get(conf);
        if (fs.exists(tmpDir)) {
            throw new IOException("Tmp directory " + fs.makeQualified(tmpDir)
                    + " already exists.  Please remove it first.");
        }
        if (!fs.mkdirs(inDir)) {
            throw new IOException("Cannot create input directory " + inDir);
        }
        
        WekaExperimentInput.generateJobs(conf, inDir, new File(args[0]));


        if(job.waitForCompletion(true))
        {
            WekaReduce.convertToReadible(conf, outDir, new File(args[1]));
            
            fs.delete(tmpDir, true);
        }
        else
        {
            System.err.println("Error!");
            System.exit(-1);
        }
        
       
    }
}
