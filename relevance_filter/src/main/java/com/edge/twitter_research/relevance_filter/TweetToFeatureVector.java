package com.edge.twitter_research.relevance_filter;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.schema.KijiURI;

import java.io.IOException;

public class TweetToFeatureVector extends Configured {

    public MapReduceJob mapReduceJob = null;

    public static Logger logger =
            Logger.getLogger(TweetToFeatureVector.class);

    public TweetToFeatureVector (String outputFilePath,
                       String inputTableName,
                       float samplingRate,
                       String dataSetType){

        PropertyConfigurator.configure(this.getClass().getResourceAsStream(Constants.LOG4J_PROPERTIES_FILE_PATH));

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));

            hBaseConfiguration.setFloat("sampling.rate", samplingRate);
            hBaseConfiguration.set("mapred.textoutputformat.separator", "|");
            hBaseConfiguration.setInt("hbase.client.scanner.caching", 1000);
            hBaseConfiguration.setBoolean("generating.training.set", dataSetType.equals("training"));

            KijiURI tableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", inputTableName)).build();

            this.mapReduceJob = KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(TweetToFeatureVectorGatherer.class)
                    .withInputTable(tableUri)
                    .withOutput(new TextMapReduceJobOutput(new Path(outputFilePath), 1))
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
            System.out.println("Usage: TweetToFeatureVector " +
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

        TweetToFeatureVector tweetToFeatureVector =
                new TweetToFeatureVector(HDFSOutputFilePath,
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
