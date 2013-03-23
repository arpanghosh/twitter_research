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

public class TweetToCSV extends Configured {

    public MapReduceJob mapReduceJob = null;

    public static Logger logger =
            Logger.getLogger(TweetToCSV.class);

    public static int tweetIDInKeyIndex = 0;


    public TweetToCSV (String outputFilePath,
                       String log4jPropertiesFilePath,
                       String inputTableName){

        PropertyConfigurator.configure(log4jPropertiesFilePath);

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));
            //hBaseConfiguration.setInt("hbase.client.scanner.caching", 500);

            setConf(hBaseConfiguration);

            KijiURI tableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", inputTableName)).build();

            this.mapReduceJob = KijiGatherJobBuilder.create()
                    .withConf(getConf())
                    .withGatherer(TweetToCSVGatherer.class)
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
            System.out.println("Usage: TweetToCSV " +
                    "<relevance_filter_root> " +
                    "<input_table_name> " +
                    "<HDFS_output_file_path>");
            return;
        }

        String relevanceFilterRoot = args[0];
        String inputTableName = args[1];
        String HDFSOutputFilePath = args[2];

        tweetIDInKeyIndex = 0;
        if (inputTableName.equals("category_tweet_store"))
            tweetIDInKeyIndex = 1;

        TweetToCSV tweetToCSV =
                new TweetToCSV(HDFSOutputFilePath,
                        relevanceFilterRoot + "/" + Constants.LOG4J_PROPERTIES_FILE_PATH,
                        inputTableName);

        boolean isSuccessful = false;
        if (tweetToCSV.mapReduceJob != null){
            try{
                isSuccessful = tweetToCSV.mapReduceJob.run();
            }catch (Exception unknownException){
                logger.error("Unknown Exception while running MapReduce Job", unknownException);
                System.exit(1);
            }
        }

        String result = isSuccessful ? "Successful" : "Failure";
        logger.info(result);
    }

}
