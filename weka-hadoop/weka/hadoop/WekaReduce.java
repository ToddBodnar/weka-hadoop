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
           writer.append(new Text(key.toString()), new Text(values.iterator().next().toString()));
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
      
      PrintWriter out = new PrintWriter(file.toString()+"_unsorted");
      
      
      
      out.println("Key_Run,Key_Fold,Key_Dataset,Key_Scheme,Key_Scheme_options,Key_Scheme_version_ID,Date_time,Number_of_training_instances,Number_of_testing_instances,Number_correct,Number_incorrect,Number_unclassified,Percent_correct,Percent_incorrect,Percent_unclassified,Kappa_statistic,Mean_absolute_error,Root_mean_squared_error,Relative_absolute_error,Root_relative_squared_error,SF_prior_entropy,SF_scheme_entropy,SF_entropy_gain,SF_mean_prior_entropy,SF_mean_scheme_entropy,SF_mean_entropy_gain,KB_information,KB_mean_information,KB_relative_information,True_positive_rate,Num_true_positives,False_positive_rate,Num_false_positives,True_negative_rate,Num_true_negatives,False_negative_rate,Num_false_negatives,IR_precision,IR_recall,F_measure,Matthews_correlation,Area_under_ROC,Area_under_PRC,Weighted_avg_true_positive_rate,Weighted_avg_false_positive_rate,Weighted_avg_true_negative_rate,Weighted_avg_false_negative_rate,Weighted_avg_IR_precision,Weighted_avg_IR_recall,Weighted_avg_F_measure,Weighted_avg_matthews_correlation,Weighted_avg_area_under_ROC,Weighted_avg_area_under_PRC,Unweighted_macro_avg_F_measure,Unweighted_micro_avg_F_measure,Elapsed_Time_training,Elapsed_Time_testing,UserCPU_Time_training,UserCPU_Time_testing,Serialized_Model_Size,Serialized_Train_Set_Size,Serialized_Test_Set_Size,Coverage_of_Test_Cases_By_Regions,Size_of_Predicted_Regions,Summary");
      
      Text key = new Text();
      Text value = new Text();
      
      while(reader.next(key, value))
      {
          out.println(key.toString().replaceAll("\"", "\\\"")+","+value.toString().replaceAll("\"", "\\\""));
      }
      out.close();
      
      
      //sort so weka can actually read the results
      Process sortOutput = Runtime.getRuntime().exec(new String[] { "/bin/bash", "-c", "cat "+file.toString()+"_unsorted | perl -e 'print scalar <>, sort <>;' "+file.toString() });
      int sortvalue = -99999;
       try {
           sortvalue = sortOutput.waitFor();
       } catch (InterruptedException ex) {
           Logger.getLogger(WekaReduce.class.getName()).log(Level.SEVERE, null, ex);
       }
      if(sortvalue != 0)
      {
          System.err.println("Error sorting:"+sortvalue);
      }
      else
      {
          (new File(file.toString()+"_unsorted")).delete();
      }
      
      
   }
   
   
}
