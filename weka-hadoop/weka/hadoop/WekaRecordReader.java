/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop;

import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author toddbodnar
 */
public class WekaRecordReader extends RecordReader{

    WekaJob job;
    boolean done = false;
    
    public WekaRecordReader(WekaJob j)
    {
        job = j;
        utils.LOG.info("f1");
    }
    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        done = false;
        utils.LOG.info("f2");
        job = (WekaJob) is;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        utils.LOG.info("f3");
        if(!done)
        {
            done = true;
            return true;
        }
        return false;
        
       }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        utils.LOG.info("f4");
        done = true;
        return job;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        utils.LOG.info("f5");
        return job;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
       ;
    }
    
}
