/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop;

import java.io.IOException;
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
       utils.LOG.info("g1");
   }

    
   public void cleanup(Context context) throws IOException {
      //write output to a file
      Configuration conf = context.getConfiguration();
      Path outDir = new Path(conf.get(FileOutputFormat.OUTDIR));
      Path outFile = new Path(outDir, "reduce-out");
      FileSystem fileSys = FileSystem.get(conf);
      SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
          outFile, Text.class, Text.class, 
          CompressionType.NONE);
      writer.append(new Text("Test Key"), new Text("Test Value"));
      writer.close();
    }
}
