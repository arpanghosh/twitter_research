package com.edge.twitter_research.relevance_filter;


import com.edge.twitter_research.core.CrisisMailer;
import org.apache.log4j.Logger;
import weka.attributeSelection.InfoGainAttributeEval;
import weka.attributeSelection.Ranker;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.BayesNet;
import weka.classifiers.rules.JRip;
import weka.classifiers.rules.Prism;
import weka.core.Instances;
import weka.core.converters.CSVLoader;
import weka.filters.Filter;
import weka.filters.supervised.attribute.Discretize;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;


public class RIPPERTweetRelevanceFilterTweet {

    public static void main(String[] args){

        if (args.length < 1){
            System.out.println("usage: RIPPERTweetRelevanceFilterTweet <root_path>");
            System.exit(-1);
        }

        String rootPath = args[0];
        File dataFolder = new File(rootPath + "/data/tweet");
        String resultFolderPath = rootPath + "/results/tweet/RIPPER/";

        CrisisMailer crisisMailer = CrisisMailer.getCrisisMailer();
        Logger logger = Logger.getLogger(RIPPERTweetRelevanceFilterTweet.class);

        File resultFolder = new File(resultFolderPath);
        if (!resultFolder.exists())
            resultFolder.mkdir();


        CSVLoader csvLoader = new CSVLoader();

        try{
            for (File dataSetName : dataFolder.listFiles()){

                Instances data = null;
                try{
                    csvLoader.setSource(dataSetName);
                    csvLoader.setNominalAttributes("19");
                    csvLoader.setNominalAttributes("2-4,8-12,14-19");
                    data = csvLoader.getDataSet();
                }catch (IOException ioe){
                    logger.error(ioe);
                    crisisMailer.sendEmailAlert(ioe);
                    System.exit(-1);
                }

                data.setClassIndex(data.numAttributes() - 1);
                data.deleteWithMissingClass();
                data.deleteAttributeAt(0);

                Discretize discretize = new Discretize();
                discretize.setInputFormat(data);
                Instances discretizedData = Filter.useFilter(data, discretize);


                FileOutputStream resultOutputStream =
                        new FileOutputStream(new File(resultFolderPath + dataSetName.getName()));

                resultOutputStream.write(discretizedData.toSummaryString().getBytes());




                JRip jRip = new JRip();

                try{
                    Evaluation eval = new Evaluation(discretizedData);
                    eval.crossValidateModel(jRip, discretizedData, 5,
                            new Random(System.currentTimeMillis()));



                    resultOutputStream.write(eval.toSummaryString("=== Summary ===", false).getBytes());
                    resultOutputStream.write(eval.toMatrixString().getBytes());
                    resultOutputStream.write(eval.toClassDetailsString().getBytes());



                    InfoGainAttributeEval infoGainAttributeEval = new InfoGainAttributeEval();
                    infoGainAttributeEval.buildEvaluator(discretizedData);

                    Ranker ranker = new Ranker();
                    ranker.search(infoGainAttributeEval, discretizedData);

                    for (double[] attribute : ranker.rankedAttributes()){
                        for (double smt : attribute){
                            resultOutputStream.write(String.format("%f ", smt).getBytes());
                        }
                        resultOutputStream.write("\n".getBytes());
                    }

                    resultOutputStream.close();



                }catch (Exception exception){
                    logger.error(exception);
                    crisisMailer.sendEmailAlert(exception);
                    System.exit(-1);
                }

            }
        }catch (Exception exception){
            logger.error(exception);
            exception.printStackTrace();
            crisisMailer.sendEmailAlert(exception);
            System.out.println(-1);
        }
    }
}