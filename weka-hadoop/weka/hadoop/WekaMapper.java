/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.core.Instances;
import weka.core.converters.ConverterUtils;

/**
 *
 * @author toddbodnar
 */
//public class WekaMapper extends Mapper<LongWritable, WekaJob, WekaJob, WekaFoldResults>{

public class WekaMapper extends Mapper<LongWritable, WekaJob, Text, WekaFoldResults>{

    public static Random random = new Random();
    @Override
    public void map(LongWritable key, WekaJob value, Context context) throws IOException, InterruptedException {
        try {
            utils.LOG.info("e1");
            System.out.println("I will now run "+value.toString());
            Classifier classifier = value.classifier;
            
            Path dataset =new Path("hdfs://quickstart.cloudera:8020/user/cloudera/testdata.arff");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            
            
                        
            //ConverterUtils.DataSource source = new ConverterUtils.DataSource(value.dataset.toString());
            
            ConverterUtils.DataSource source = new ConverterUtils.DataSource(fs.open(dataset));
            
            
            Instances data = source.getDataSet();
            
            if(data.classIndex() < 0)
                data.setClassIndex(data.numAttributes()-1);
            
            classifier.buildClassifier(data);
            
             Evaluation eval;
        
            eval = new Evaluation(data);
            eval.crossValidateModel(classifier, data, 10, random);
            
            String results = eval.toSummaryString();
            
            WekaFoldResults wfs = new WekaFoldResults(results);
            
            
            
            //context.write(value,wfs);
            context.write(new Text(utils.classifierToString(value.classifier)+","+value.dataset+","+value.key),wfs);
        } catch (Exception ex) {
            Logger.getLogger(WekaMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    
   
}
