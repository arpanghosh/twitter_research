package com.edge.twitter_research.event_detection;


import com.edge.twitter_research.core.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.kiji.mapreduce.KijiMapReduceJob;

import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.mapreduce.produce.KijiProduceJobBuilder;
import org.kiji.schema.KijiURI;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class LabelTweetsByCompanyAndArea extends Configured {

    public KijiMapReduceJob mapReduceJob = null;

    public static Logger logger =
            Logger.getLogger(LabelTweetsByCompanyAndArea.class);

    public LabelTweetsByCompanyAndArea (String tableName){

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));


            KijiURI tableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", tableName)).build();

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

            this.mapReduceJob = KijiProduceJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withInputTable(tableUri)
                    .withProducer(LabelTweetsByCompanyProducer.class)
                    .withOutput(new DirectKijiTableMapReduceJobOutput(tableUri,1))
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

        if (args.length < 1){
            System.out.println("Usage: LabelTweetsByCompanyAndArea " +
                    "<table_name>");
            return;
        }

        String tableName = args[0];


        LabelTweetsByCompanyAndArea labelTweetsByCompanyAndArea =
                new LabelTweetsByCompanyAndArea(tableName);

        boolean isSuccessful = false;
        if (labelTweetsByCompanyAndArea.mapReduceJob != null){
            try{
                isSuccessful = labelTweetsByCompanyAndArea.mapReduceJob.run();
            }catch (Exception unknownException){
                logger.error("Unknown Exception while running MapReduce Job", unknownException);
                System.exit(1);
            }
        }

        String result = isSuccessful ? "Successful" : "Failure";
        logger.info(result);
    }

}

