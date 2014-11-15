/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weka.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import weka.classifiers.Classifier;
import weka.core.OptionHandler;
import weka.core.Utils;

/**
 *
 * @author toddbodnar
 */
public class utils {
    public static String classifierToString(Classifier c)
    {
        String str = c.getClass().getName();
		  if (c instanceof OptionHandler)
		    str += " " + Utils.joinOptions(((OptionHandler) c).getOptions());
        return str;
    }
    public static final Log LOG = LogFactory.getLog(utils.class);
    
    
}
