    package com.edge.twitter_research.relevance_filter;


import com.edge.twitter_research.core.CrisisMailer;
import org.apache.log4j.Logger;
import weka.classifiers.Evaluation;

import weka.classifiers.functions.LinearRegression;
import weka.classifiers.functions.SimpleLinearRegression;
import weka.classifiers.functions.SimpleLogistic;
import weka.classifiers.trees.lmt.LogisticBase;
import weka.core.Instances;
import weka.core.converters.CSVLoader;
import weka.filters.Filter;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;


public class LinearRegressionTweetRelevanceFilterLanguage {

    public static void main(String[] args){

        if (args.length < 1){
            System.out.println("usage: LinearRegressionTweetRelevanceFilterLanguage <root_path>");
            System.exit(-1);
        }

        String rootPath = args[0];
        File dataFolder = new File(rootPath + "/data");
        String resultFolderPath = rootPath + "/results/Regression/";

        CrisisMailer crisisMailer = CrisisMailer.getCrisisMailer();
        Logger logger = Logger.getLogger(LinearRegressionTweetRelevanceFilterLanguage.class);

        File resultFolder = new File(resultFolderPath);
        if (!resultFolder.exists())
            resultFolder.mkdir();


        CSVLoader csvLoader = new CSVLoader();

        try{
            for (File dataSetName : dataFolder.listFiles()){

                Instances data = null;
                try{
                    csvLoader.setSource(dataSetName);
                    data = csvLoader.getDataSet();
                    System.out.println(data);
                }catch (IOException ioe){
                    ioe.printStackTrace();
                    //crisisMailer.sendEmailAlert(ioe);
                    System.exit(-1);
                }

                data.setClassIndex(data.numAttributes() - 1);
                data.deleteWithMissingClass();


                SimpleLogistic logisticRegressionClassifier = new SimpleLogistic();

                try{
                    Evaluation eval = new Evaluation(data);
                    eval.crossValidateModel(logisticRegressionClassifier, data, 2,
                            new Random(System.currentTimeMillis()));

                    /*
                    FileOutputStream resultOutputStream =
                            new FileOutputStream(new File(resultFolderPath + dataSetName.getName()));
                    */
                    /*
                    resultOutputStream.write(eval.toSummaryString("=== Summary ===", false).getBytes());
                    resultOutputStream.write(eval.toMatrixString().getBytes());
                    resultOutputStream.write(eval.toClassDetailsString().getBytes());
                    resultOutputStream.close();
                    */

                    System.out.println(eval.toSummaryString("=== Summary ===", false).getBytes());
                    System.out.println(eval.toMatrixString().getBytes());
                    System.out.println(eval.toClassDetailsString().getBytes());


                }catch (Exception exception){
                    exception.printStackTrace();
                    //crisisMailer.sendEmailAlert(exception);
                    System.exit(-1);
                }

            }
        }catch (Exception exception){
            logger.error(exception);
            crisisMailer.sendEmailAlert(exception);
            System.out.println(-1);
        }
    }
}
