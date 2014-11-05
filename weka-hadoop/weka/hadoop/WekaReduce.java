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
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author toddbodnar
 */
public class WekaReduce  extends Reducer<WekaJob, WekaFoldResults,Text, Text>{

   @Override
   public void reduce(WekaJob key, Iterable<WekaFoldResults> values,Context context)
   {
       utils.LOG.info("g1");
   }

    
   
}
