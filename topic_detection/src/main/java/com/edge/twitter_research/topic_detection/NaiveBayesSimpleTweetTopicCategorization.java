package com.edge.twitter_research.topic_detection;

import com.edge.twitter_research.core.CrisisMailer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayesSimple;
import weka.core.Instances;
import weka.core.converters.CSVLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.StringToWordVector;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;



public class NaiveBayesSimpleTweetTopicCategorization {

    public static void main(String[] args){

        if (args.length < 1){
            System.out.println("usage: NaiveBayesSimpleTweetTopicCategorization <root_path>");
            System.exit(-1);
        }

        String rootPath = args[0];
        File dataFolder = new File(rootPath + "/data");
        String resultFolderPath = rootPath + "/results/NaiveBayesSimple/";

        CrisisMailer crisisMailer = CrisisMailer.getCrisisMailer();
        Logger logger = Logger.getLogger(NaiveBayesSimpleTweetTopicCategorization.class);
        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        File resultFolder = new File(resultFolderPath);
        if (!resultFolder.exists())
            resultFolder.mkdir();


        CSVLoader csvLoader = new CSVLoader();

        try{
            for (File dataSetName : dataFolder.listFiles()){

                Instances data = null;
                try{
                    csvLoader.setSource(dataSetName);
                    csvLoader.setStringAttributes("2");
                    data = csvLoader.getDataSet();
                }catch (IOException ioe){
                    logger.error(ioe);
                    crisisMailer.sendEmailAlert(ioe);
                    System.exit(-1);
                }

                data.setClassIndex(data.numAttributes() - 1);
                data.deleteWithMissingClass();

                Instances vectorizedData = null;
                StringToWordVector stringToWordVectorFilter = new StringToWordVector();
                try{
                    stringToWordVectorFilter.setInputFormat(data);
                    stringToWordVectorFilter.setAttributeIndices("2");
                    stringToWordVectorFilter.setIDFTransform(true);
                    stringToWordVectorFilter.setLowerCaseTokens(true);
                    stringToWordVectorFilter.setOutputWordCounts(false);
                    stringToWordVectorFilter.setUseStoplist(true);

                    vectorizedData = Filter.useFilter(data, stringToWordVectorFilter);
                    vectorizedData.deleteAttributeAt(0);
                    //System.out.println(vectorizedData);
                }catch (Exception exception){
                    logger.error(exception);
                    crisisMailer.sendEmailAlert(exception);
                    System.exit(-1);
                }

                NaiveBayesSimple naiveBayesSimpleClassifier = new NaiveBayesSimple();

                /*
                FilteredClassifier filteredClassifier = new FilteredClassifier();
                filteredClassifier.setFilter(stringToWordVectorFilter);
                filteredClassifier.setClassifier(naiveBayesMultinomialClassifier);
                */

                try{
                    Evaluation eval = new Evaluation(vectorizedData);
                    eval.crossValidateModel(naiveBayesSimpleClassifier, vectorizedData, 10,
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
