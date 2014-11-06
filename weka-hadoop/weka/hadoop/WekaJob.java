/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import weka.classifiers.Classifier;
import weka.core.Capabilities;
import weka.core.Instance;
import weka.core.Instances;

/**
 *
 * @author toddbodnar
 */
class WekaJob extends InputSplit implements Writable, Serializable{
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

    @Override
    public void write(DataOutput d) throws IOException {
        utils.LOG.info("d4");
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);   
        out.writeObject(this);
        d.write(bos.toByteArray());
        
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        utils.LOG.info("d5");
        
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
