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
import weka.experiment.Experiment;

/**
 *
 * @author toddbodnar
 */
public class WekaExperimentInput <K, V> extends InputFormat<K, V>{

    static HashMap<JobContext, Experiment> settings = new HashMap<JobContext,Experiment>();
    /**
     * 
     * @param infile A weka experiment config file
     */
    public static void setInfile(JobContext jc, File infile) throws Exception
    {
        settings.put(jc,Experiment.read(infile.toString()));
    }
    @Override
    public List<InputSplit> getSplits(JobContext jc) throws IOException, InterruptedException {
        
        Experiment exp = settings.get(jc);
        
        ArrayList<InputSplit> jobs = new ArrayList<InputSplit>();
        
        while (exp.hasMoreIterations()) {
      
            System.out.println(exp.getCurrentDatasetNumber()+","+exp.getCurrentPropertyNumber()+","+exp.getCurrentRunNumber());
	exp.advanceCounters(); // Try to keep plowing through
      
        }
                
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    
  
}
