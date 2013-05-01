package com.edge.twitter_research.relevance_filter;


import com.edge.twitter_research.core.CrisisMailer;
import org.apache.log4j.Logger;
import weka.classifiers.Evaluation;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.classifiers.functions.SMO;
import weka.core.Instances;
import weka.core.converters.CSVLoader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class SVMTweetRelevanceFilterLanguage {

    public static void main(String[] args){

        if (args.length < 1){
            System.out.println("usage: SVMTweetRelevanceFilterLanguage <root_path>");
            System.exit(-1);
        }

        String rootPath = args[0];
        File dataFolder = new File(rootPath + "/data/language");
        String resultFolderPath = rootPath + "/results/language/SVM/";

        CrisisMailer crisisMailer = CrisisMailer.getCrisisMailer();
        Logger logger = Logger.getLogger(SVMTweetRelevanceFilterLanguage.class);

        File resultFolder = new File(resultFolderPath);
        if (!resultFolder.exists())
            resultFolder.mkdir();


        CSVLoader csvLoader = new CSVLoader();

        try{
            for (File dataSetName : dataFolder.listFiles()){

                Instances data = null;
                try{
                    csvLoader.setSource(dataSetName);
                    csvLoader.setNominalAttributes("22");
                    data = csvLoader.getDataSet();

                }catch (IOException ioe){
                    logger.error(ioe);
                    crisisMailer.sendEmailAlert(ioe);
                    System.exit(-1);
                }

                data.setClassIndex(data.numAttributes() - 1);
                data.deleteWithMissingClass();
                data.deleteAttributeAt(0);

                //System.out.println(data);


                SMO svmClassifier = new SMO();

                try{
                    Evaluation eval = new Evaluation(data);
                    eval.crossValidateModel(svmClassifier, data, 5,
                            new Random(System.currentTimeMillis()));


                    FileOutputStream resultOutputStream =
                            new FileOutputStream(new File(resultFolderPath + dataSetName.getName()));

                    resultOutputStream.write(eval.toSummaryString("=== Summary ===", false).getBytes());
                    resultOutputStream.write(eval.toMatrixString().getBytes());
                    resultOutputStream.write(eval.toClassDetailsString().getBytes());
                    resultOutputStream.close();


                }catch (Exception exception){
                    logger.error(exception);
                    crisisMailer.sendEmailAlert(exception);
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

