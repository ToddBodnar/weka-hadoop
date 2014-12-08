/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop.tools;

import java.io.File;
import java.util.LinkedList;
import javax.swing.DefaultListModel;
import weka.classifiers.Classifier;
import weka.experiment.Experiment;
import weka.filters.Filter;

/**
 * creates an experiment with a combination of settings
 * @author toddbodnar
 */
public class combination {
    public static void main(String args[]) throws Exception
    {
        Experiment exp = new Experiment(); //new Experiment doesn't actually init a new experiment...
        
        File output = new File(args[0]);
        
        exp = Experiment.read(output.toString());//so load in another
        
        
        
        //DefaultListModel classifiers = new DefaultListModel();
        //classifiers.addElement(new weka.classifiers.bayes.NaiveBayes());
        Classifier classifiers[] = {new weka.classifiers.bayes.NaiveBayes()};
        
        LinkedList<Classifier> baseClassifiers = new LinkedList<Classifier>();
        
        baseClassifiers.add(new weka.classifiers.rules.ZeroR());
        
        baseClassifiers.add(new weka.classifiers.trees.J48());
        baseClassifiers.add(new weka.classifiers.trees.RandomTree());
        baseClassifiers.add(new weka.classifiers.trees.RandomForest());
        
        weka.classifiers.functions.MultilayerPerceptron ANN = new weka.classifiers.functions.MultilayerPerceptron();
        ANN.setDebug(true);
        ANN.setLearningRate(.03);
        ANN.setTrainingTime(500);
        ANN.setValidationSetSize(20);
        baseClassifiers.add(ANN);
        
        weka.classifiers.functions.SMO SMO = new weka.classifiers.functions.SMO();
        SMO.setBuildLogisticModels(true);
        SMO.setKernel(new weka.classifiers.functions.supportVector.PolyKernel());
        baseClassifiers.add(SMO);
        SMO = new weka.classifiers.functions.SMO();
        SMO.setBuildLogisticModels(true);
        SMO.setKernel(new weka.classifiers.functions.supportVector.RBFKernel());
        baseClassifiers.add(SMO);
        
        baseClassifiers.add(new weka.classifiers.bayes.BayesNet());
        baseClassifiers.add(new weka.classifiers.bayes.NaiveBayes());
        
        baseClassifiers.add(new weka.classifiers.functions.Logistic());
        
        baseClassifiers.add(new weka.classifiers.meta.AdaBoostM1());
        
        for(int nn : new int[]{1,2,4,8,16,32})
        {
            weka.classifiers.lazy.IBk KNN = new weka.classifiers.lazy.IBk();
            KNN.setKNN(nn);
            baseClassifiers.add(KNN);
        }
        
        System.out.println(baseClassifiers.size()+" base Classifiers");
        
        LinkedList<Filter> filters = new LinkedList<Filter>();
        
        weka.core.tokenizers.Tokenizer tokenizers[] = new weka.core.tokenizers.Tokenizer[6];
        for(int ct=0;ct<6;ct+=2)
        {
            weka.core.tokenizers.NGramTokenizer ngram = new weka.core.tokenizers.NGramTokenizer();
            ngram.setDelimiters("\t \n\r!@#\\$%\\^\\&\\*()\\<\\>,\\./\\?\\\"\\\\';:\\-_\\+=`~\\|\\[\\]\\{\\}");
            ngram.setNGramMaxSize(ct/2+1);
            tokenizers[ct] = ngram;
            
            ngram = new weka.core.tokenizers.NGramTokenizer();
            ngram.setDelimiters("\t \n\r!@#\\$%\\^\\&\\*()\\<\\>,\\./\\?\\\"\\\\';:\\-_\\+=`~\\|\\[\\]\\{\\}1234567890");
            ngram.setNGramMaxSize(ct/2+1);
            tokenizers[ct+1] = ngram;
        }
        
        for(boolean stop : new boolean[]{false})
        for(int minTermFreq : new int[]{2,5,10,20,50,100,200})
        {
        for(weka.core.tokenizers.Tokenizer token:tokenizers)
        {
            weka.filters.unsupervised.attribute.StringToWordVector stwv = new weka.filters.unsupervised.attribute.StringToWordVector();
            stwv.setLowerCaseTokens(true);
            stwv.setTokenizer(token);
            stwv.setMinTermFreq(minTermFreq);
            stwv.setAttributeNamePrefix("keyword_");
            stwv.setUseStoplist(stop);
            filters.add(stwv);
        }
        }
        
        
        System.out.println(filters.size()+" filters");
        
        LinkedList<Classifier> classifierlist = new LinkedList<Classifier>();
        for(Filter filter:filters)
        {
            for(Classifier classifier:baseClassifiers)
            {
                weka.classifiers.meta.FilteredClassifier finalclass = new weka.classifiers.meta.FilteredClassifier();
                finalclass.setFilter(filter);
                finalclass.setClassifier(classifier);
                classifierlist.add(finalclass);
            }
        }
        
        System.out.println(classifierlist.size()+" total classifiers");
        
        Classifier classifierarray[] = new Classifier[classifierlist.size()];
        for(int ct=0;ct<classifierlist.size();ct++)
        {
            classifierarray[ct] = classifierlist.get(ct);
        }
        
        exp.setPropertyArray(classifierarray);
        
        exp.toString();
        
       
        Experiment.write(output.toString(), exp);
    }
}
