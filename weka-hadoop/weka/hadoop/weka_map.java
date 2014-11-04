/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author toddbodnar
 */
public class weka_map  extends MapReduceBase implements Mapper<LongWritable, WekaJob, WekaJob, WekaFoldResults>{

    @Override
    public void map(LongWritable key, WekaJob value, OutputCollector<WekaJob, WekaFoldResults> output, Reporter reporter) throws IOException {
        System.out.println("I will now pretend to run "+value);
        
        WekaFoldResults wfs = new WekaFoldResults();
        //todo: implement results
        output.collect(value,wfs);
    }

    
   
}
