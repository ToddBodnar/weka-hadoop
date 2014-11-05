/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import weka.classifiers.Classifier;

/**
 *
 * @author toddbodnar
 */
class WekaJob extends InputSplit{
    public Classifier classifier;
    public File dataset;
    
    public WekaJob(Classifier classifier, File dataset)
    {
        utils.LOG.info("d1");
        this.classifier = classifier;
        this.dataset = dataset;
    }
    public String toString()
    {
        return classifier.toString() + " on " + dataset.toString();
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        utils.LOG.info("d2");
        return 1;//dataset.length();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        utils.LOG.info("d3");
        return new String[]{};//dataset.toString()};
    }
}
