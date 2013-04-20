package com.edge.twitter_research.relevance_filter;


import com.edge.twitter_research.core.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.schema.KijiURI;

import java.io.IOException;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class TweetToFeatureVectorConverter extends Configured {

    public KijiMapReduceJob mapReduceJob = null;

    public static Logger logger =
            Logger.getLogger(TweetToFeatureVectorConverter.class);

    public TweetToFeatureVectorConverter (String outputFilePath,
                       String inputTableName,
                       float samplingRate,
                       String dataSetType){

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));

            hBaseConfiguration.setFloat("sampling.rate", samplingRate);
            hBaseConfiguration.set("mapred.textoutputformat.separator", "|");
            hBaseConfiguration.setInt("hbase.client.scanner.caching", 1000);
            hBaseConfiguration.setBoolean("generating.training.set", dataSetType.equals("training"));

            KijiURI tableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", inputTableName)).build();

            String additionalJarsPath = "";

            try{
                additionalJarsPath = InetAddress.getLocalHost().getHostName().equals("master")?
                                        GlobalConstants.ADDTIONAL_JARS_PATH_KIJI_CLUSTER :
                                        GlobalConstants.ADDTIONAL_JARS_PATH_BENTO;
            }catch (UnknownHostException unknownHostException){
                logger.error(unknownHostException);
                unknownHostException.printStackTrace();
                System.exit(-1);
            }

            this.mapReduceJob = KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(TweetToFeatureVectorGatherer.class)
                    .withInputTable(tableUri)
                    .withOutput(new TextMapReduceJobOutput(new Path(outputFilePath), 1))
                    .addJarDirectory(new Path(additionalJarsPath))
                    .build();
        }catch (IOException ioException){
            logger.error("IO Exception while configuring MapReduce Job", ioException);
            System.exit(1);
        } catch (Exception unknownException){
            logger.error("Unknown Exception while configuring MapReduce Job", unknownException);
            System.exit(1);
        }
    }


    public static void main(String[] args){

        if (args.length < 3){
            System.out.println("Usage: TweetToFeatureVectorConverter " +
                    "<input_table_name> " +
                    "<HDFS_output_file_path> " +
                    "<data_type (testing or training)> " +
                    "<sampling_rate (%)>");
            return;
        }

        String dataSet = args[2];
        if (!dataSet.equals("training") && !dataSet.equals("testing")){
            System.out.println("Enter a valid dataset type");
            return;
        }


        String inputTableName = args[0];
        String HDFSOutputFilePath = args[1];
        float samplingRate;
        if (dataSet.equals("training"))
            samplingRate = 100;
        else if (args.length > 3)
            samplingRate = Float.parseFloat(args[3]);
        else
            samplingRate = 50;

        TweetToFeatureVectorConverter tweetToFeatureVector =
                new TweetToFeatureVectorConverter(HDFSOutputFilePath,
                        inputTableName,
                        samplingRate,
                        dataSet);

        boolean isSuccessful = false;
        if (tweetToFeatureVector.mapReduceJob != null){
            try{
                isSuccessful = tweetToFeatureVector.mapReduceJob.run();
            }catch (Exception unknownException){
                logger.error("Unknown Exception while running MapReduce Job", unknownException);
                System.exit(1);
            }
        }

        String result = isSuccessful ? "Successful" : "Failure";
        logger.info(result);
    }

}
