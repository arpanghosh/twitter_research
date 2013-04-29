package com.edge.twitter_research.queries;


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
import java.util.ArrayList;

public class TweetOriginalityCalculator extends Configured {

    public ArrayList<KijiMapReduceJob> mapReduceJobs;

    public static Logger logger =
            Logger.getLogger(PerTimeTweetVolumeForAllCompanies.class);

    public TweetOriginalityCalculator (String rootFilePath, int numReducers, float samplingRate){

        mapReduceJobs = new ArrayList<KijiMapReduceJob>();

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));
            hBaseConfiguration.setFloat("sampling.rate", samplingRate);
            hBaseConfiguration.set("mapred.textoutputformat.separator", ",");
            //hBaseConfiguration.setInt("hbase.client.scanner.caching", 1000);

            Path categoryResultFilePath = new Path(rootFilePath + "/result/" + "category_tweet_store");
            Path sampleResultFilePath = new Path(rootFilePath + "/result/" + "sample_tweet_store");

            KijiURI categoryTableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", "category_tweet_store")).build();
            KijiURI sampleTableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", "sample_tweet_store")).build();

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


            mapReduceJobs.add(KijiGatherJobBuilder.create()
                        .withConf(hBaseConfiguration)
                        .withGatherer(UsersGatherer.class)
                        .withReducer(PerUserCSVGenerator.class)
                        .withInputTable(categoryTableUri)
                        .withOutput(new TextMapReduceJobOutput(categoryResultFilePath, numReducers))
                        .addJarDirectory(new Path(additionalJarsPath))
                        .build());



            mapReduceJobs.add(KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(UsersGatherer.class)
                    .withReducer(PerUserCSVGenerator.class)
                    .withInputTable(sampleTableUri)
                    .withOutput(new TextMapReduceJobOutput(sampleResultFilePath, numReducers))
                    .addJarDirectory(new Path(additionalJarsPath))
                    .build());


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
            System.out.println("Usage: TweetOriginalityCalculator " +
                    "<HDFS_job_root_file_path> " +
                    "<num_reducers> " +
                    "<sampling_rate>");
            return;
        }


        String HDFSjobRootFilePath = args[0];
        int numReducers = Integer.parseInt(args[1]);
        float samplingRate = Float.parseFloat(args[3]);


        TweetOriginalityCalculator tweetOriginalityCalculator =
                new TweetOriginalityCalculator(HDFSjobRootFilePath, numReducers, samplingRate);


        boolean isSuccessful = false;

        for (KijiMapReduceJob mapReduceJob : tweetOriginalityCalculator.mapReduceJobs){

            if (mapReduceJob != null){
                try{
                    isSuccessful = mapReduceJob.run();
                    if (!isSuccessful)
                        break;
                }catch (Exception unknownException){
                    logger.error("Unknown Exception while running MapReduce Job", unknownException);
                    System.exit(1);
                }
            }
        }

        String result = isSuccessful ? "Successful" : "Failure";
        logger.info(result);
    }

}

