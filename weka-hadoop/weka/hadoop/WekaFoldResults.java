/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import weka.classifiers.Evaluation;
import weka.core.Instances;

/**
 *
 * @author toddbodnar
 */
class WekaFoldResults extends Text{

    WekaFoldResults()
    {
        super();
    }
    WekaFoldResults(String toString) {
        super(toString);
    }
    
    WekaFoldResults(Evaluation eval, Instances train, Instances test)
    {
        super();
        try {
            String results = "'123','"+System.currentTimeMillis()+"','"+train.numInstances()+"','"+test.numInstances()+"','"+eval.correct()+"','"+eval.incorrect()+"','"+eval.unclassified()+"','"+eval.pctCorrect()+"','"+eval.pctIncorrect()+"','"+eval.pctUnclassified()+"','"+eval.kappa()+"','"+eval.meanAbsoluteError()+"','"+eval.rootMeanSquaredError()+"','"+eval.relativeAbsoluteError()+"','"+eval.rootRelativeSquaredError()+"','"+eval.SFPriorEntropy()+"','"+eval.SFSchemeEntropy()+"','"+eval.SFEntropyGain()+"','"+eval.SFMeanPriorEntropy()+eval.SFMeanSchemeEntropy()+"','"+eval.SFMeanEntropyGain()+"','"+eval.KBInformation()+"','"+eval.KBMeanInformation()+"','"+eval.KBRelativeInformation()+"','";
            results+= eval.truePositiveRate(1)+"','"+eval.numTruePositives(1)+"','"+eval.falsePositiveRate(1)+"','"+eval.numFalsePositives(1)+"','"+eval.trueNegativeRate(1)+"','"+eval.numTrueNegatives(1)+"','"+eval.falseNegativeRate(1)+"','"+eval.numFalseNegatives(1)+"','";
            results+= eval.precision(1)+"','"+eval.recall(1)+"','"+eval.fMeasure(1)+"','"+eval.matthewsCorrelationCoefficient(1)+"','"+eval.areaUnderROC(1)+"','"+eval.areaUnderPRC(1)+"','"+eval.weightedTruePositiveRate()+"','"+eval.weightedFalsePositiveRate()+"','"+eval.weightedTrueNegativeRate()+"','"+eval.weightedFalseNegativeRate()+"','"+eval.weightedPrecision()+"','"+eval.weightedRecall()+"','"+eval.weightedFMeasure()+"','"+eval.weightedMatthewsCorrelation()+"','"+eval.weightedAreaUnderROC()+"','"+eval.weightedAreaUnderPRC()+"','";
            results+= eval.unweightedMacroFmeasure()+"','"+eval.unweightedMicroFmeasure()+"','";
            results+= "1','1','1','1','1','1','1','1','1','1','1'";
            
            super.set(results);
        } catch (Exception ex) {
            Logger.getLogger(WekaFoldResults.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
    }

    /*@Override
    public void write(DataOutput d) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }*/
    
}
