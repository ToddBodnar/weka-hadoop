/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.security.Credentials;
import weka.classifiers.Classifier;
import weka.experiment.Experiment;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

/**
 *
 * @author toddbodnar
 */
public class WekaExperimentInput{

    
    static HashMap<JobContext, Experiment> settings = new HashMap<JobContext,Experiment>();
    /**
     * 
     * @param infile A weka experiment config file
     */
    public static void generateJobs(Configuration conf, Path indir, File experimentFile) throws Exception
    {
        FileSystem fs = FileSystem.get(conf);
        utils.LOG.info("Setting in file "+experimentFile);
    
        Experiment exp = Experiment.read(experimentFile.toString());
        
        Classifier properties[] = (Classifier[]) exp.getPropertyArray();
        
        exp.advanceCounters();
        
        long key_id = 1;
        
        while (exp.hasMoreIterations()) {
      
            //LOG.info(exp.getDatasets().get(exp.getCurrentDatasetNumber())+","+utils.classifierToString((Classifier) exp.getPropertyArrayValue(exp.getCurrentPropertyNumber()))+","+exp.getCurrentRunNumber());
	
            System.out.println(exp.getCurrentDatasetNumber()+","+exp.getCurrentPropertyNumber()+","+exp.getCurrentRunNumber());
            //splits.add(new WekaJob((Classifier) exp.getPropertyArrayValue(exp.getCurrentPropertyNumber()), (File) exp.getDatasets().get(exp.getCurrentDatasetNumber()),key));
            
            final Path file = new Path(indir, "part"+key_id);
            final LongWritable key = new LongWritable(key_id);
            
            //final SequenceFile.Writer writer = SequenceFile.createWriter( fs, conf, file, LongWritable.class, LongWritable.class, CompressionType.NONE);
            final SequenceFile.Writer writer = SequenceFile.createWriter( fs, conf, file, LongWritable.class, WekaJob.class, CompressionType.NONE);
        try {
          writer.append(key, new WekaJob((Classifier) exp.getPropertyArrayValue(exp.getCurrentPropertyNumber()), (File) exp.getDatasets().get(exp.getCurrentDatasetNumber()),key_id));
        } finally {
          writer.close();
        }
        //System.out.println("Wrote input for Map #"+i);
            
            exp.advanceCounters(); // Try to keep plowing through
            key_id++;
            
            if(key_id > 10) break; //for testing
        }
        
        
    }

    
    
      public static void main(String args[]) throws Exception
    {
        JobContext jc = new JobContext() {

            @Override
            public Configuration getConfiguration() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Credentials getCredentials() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public JobID getJobID() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public int getNumReduceTasks() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Path getWorkingDirectory() throws IOException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Class<?> getOutputKeyClass() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Class<?> getOutputValueClass() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Class<?> getMapOutputKeyClass() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Class<?> getMapOutputValueClass() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public String getJobName() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public RawComparator<?> getSortComparator() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public String getJar() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public RawComparator<?> getCombinerKeyGroupingComparator() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public RawComparator<?> getGroupingComparator() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public boolean getJobSetupCleanupNeeded() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public boolean getTaskCleanupNeeded() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public boolean getProfileEnabled() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public String getProfileParams() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Configuration.IntegerRanges getProfileTaskRange(boolean bln) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public String getUser() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public boolean getSymlink() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Path[] getArchiveClassPaths() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public URI[] getCacheArchives() throws IOException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public URI[] getCacheFiles() throws IOException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Path[] getLocalCacheArchives() throws IOException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Path[] getLocalCacheFiles() throws IOException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Path[] getFileClassPaths() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public String[] getArchiveTimestamps() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public String[] getFileTimestamps() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public int getMaxMapAttempts() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public int getMaxReduceAttempts() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
        
        //generateJobs(jc,new File("/Users/toddbodnar/scratch/emotion_experiments.exp"));
        
        //System.out.println((new WekaExperimentInput()).getSplits(jc).size());
        
    }
  
}
