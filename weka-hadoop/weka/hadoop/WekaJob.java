/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop;

import java.io.File;
import weka.classifiers.Classifier;

/**
 *
 * @author toddbodnar
 */
class WekaJob {
    public Classifier classifier;
    public File dataset;
    
    public String toString()
    {
        return classifier.toString() + " on " + dataset.toString();
    }
}
