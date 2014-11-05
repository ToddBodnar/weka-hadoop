/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop;

import java.io.IOException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author toddbodnar
 */
public class WekaOutput extends OutputFormat{

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext tac) throws IOException, InterruptedException {
       return new NullRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext jc) throws IOException, InterruptedException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext tac) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private class NullRecordWriter extends RecordWriter
    {

        @Override
        public void write(Object k, Object v) throws IOException, InterruptedException {
            ;
        }

        @Override
        public void close(TaskAttemptContext tac) throws IOException, InterruptedException {
            ;
        }
        
    }
    
    
}
