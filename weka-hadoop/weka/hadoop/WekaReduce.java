/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author toddbodnar
 */
//public class WekaReduce  extends Reducer<WekaJob, WekaFoldResults,WekaJob, WekaFoldResults>{
public class WekaReduce  extends Reducer<Text, WekaFoldResults,LongWritable, LongWritable>{

    

   @Override
   public void reduce(Text key, Iterable<WekaFoldResults> values,Context context)
   {
      
       Iterator<WekaFoldResults> itr = values.iterator();
       int iteration = 0;
       while(itr.hasNext()){
       try {
           writer.append(new Text(key.toString()+"_"+iteration), new Text(values.iterator().next().toString()));
       } catch (IOException ex) {
           try {
               writer.append(key, new Text(ex.toString()));
           } catch (IOException ex1) {
               Logger.getLogger(WekaReduce.class.getName()).log(Level.SEVERE, null, ex1);
           }
           Logger.getLogger(WekaReduce.class.getName()).log(Level.SEVERE, null, ex);
       }
       }
       utils.LOG.info("g1");
       
       
   }

   public void setup(Context context) throws IOException
   {
       Configuration conf = context.getConfiguration();
      Path outDir = new Path(conf.get(FileOutputFormat.OUTDIR));
      Path outFile = new Path(outDir, "reduce-out");
      FileSystem fileSys = FileSystem.get(conf);
      
      writer = SequenceFile.createWriter(fileSys, conf,
          outFile, Text.class, Text.class, 
          CompressionType.NONE);
   }
   
   SequenceFile.Writer writer;
    
   public void cleanup(Context context) throws IOException {
      //write output to a file
      
      writer.close();
    }
   
   /**
    * Converts the binary SequenceFile.writer file to one readible in weka/r/excel/etc
    * 
    * Only necessary if this is the last MapReduce step.
    * 
    * @param conf
    * @param file 
    */
   static void convertToReadible(Configuration conf, Path outDir, File file) throws IOException {
           
      Path outFile = new Path(outDir, "reduce-out");
      FileSystem fileSys = FileSystem.get(conf);
      
      SequenceFile.Reader reader = new SequenceFile.Reader(fileSys,
          outFile, conf);
      
      PrintWriter out = new PrintWriter(file);
      
      //dummy headers
      out.println("Key,Value");
      
      Text key = new Text();
      Text value = new Text();
      
      while(reader.next(key, value))
      {
          out.println(key+","+value);
      }
      out.close();
      
   }
   
   
}
