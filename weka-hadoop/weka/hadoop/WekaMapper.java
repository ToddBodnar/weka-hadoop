/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author toddbodnar
 */
//public class WekaMapper extends Mapper<LongWritable, WekaJob, WekaJob, WekaFoldResults>{

public class WekaMapper extends Mapper<LongWritable, WekaJob, Text, WekaFoldResults>{

    @Override
    public void map(LongWritable key, WekaJob value, Context context) throws IOException, InterruptedException {
        utils.LOG.info("e1");
        System.out.println("I will now pretend to run "+value.toString());
        
        WekaFoldResults wfs = new WekaFoldResults(utils.classifierToString(value.classifier)+" "+value.dataset+" "+value.key);
        try {
            //todo: implement results
            Thread.sleep(10);
        } catch (InterruptedException ex) {
            Logger.getLogger(WekaMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        //context.write(value,wfs);
        context.write(new Text(value.toString()+"KEY"),wfs);
    }

    
   
}
