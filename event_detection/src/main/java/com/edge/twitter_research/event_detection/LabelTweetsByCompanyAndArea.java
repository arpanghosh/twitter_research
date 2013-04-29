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
import org.kiji.mapreduce.output.HFileMapReduceJobOutput;
import org.kiji.mapreduce.produce.KijiProduceJobBuilder;
import org.kiji.schema.KijiURI;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class LabelTweetsByCompanyAndArea extends Configured {

    public KijiMapReduceJob mapReduceJob = null;

    public static Logger logger =
            Logger.getLogger(LabelTweetsByCompanyAndArea.class);

    public LabelTweetsByCompanyAndArea (String tableName,
                                        String jobRootPath,
                                        float samplingRate,
                                        int numReducers,
                                        int scanner){

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));
            hBaseConfiguration.setFloat("sampling.rate", samplingRate);
            hBaseConfiguration.setInt("hbase.client.scanner.caching", scanner);

            KijiURI tableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", tableName)).build();
            Path HFilePath = new Path(jobRootPath + "/hfiles");

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
                    .withOutput(new DirectKijiTableMapReduceJobOutput(tableUri))
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

        if (args.length < 5){
            System.out.println("Usage: LabelTweetsByCompanyAndArea " +
                    "<table_name> " +
                    "<HDFS_job_root_path> " +
                    "<sampling_rate> " +
                    "<num_reducers> "+
                    "scanner");
            return;
        }

        String tableName = args[0];
        String HDFSJobRootPath = args[1];
        float samplingRate = Float.parseFloat(args[2]);
        int numReducers = Integer.parseInt(args[3]);
        int scanner = Integer.parseInt(args[4]);

        LabelTweetsByCompanyAndArea labelTweetsByCompanyAndArea =
                new LabelTweetsByCompanyAndArea(tableName, HDFSJobRootPath, samplingRate, numReducers, scanner);

        long tic = 0L, toc = 0L;
        boolean isSuccessful = false;
        if (labelTweetsByCompanyAndArea.mapReduceJob != null){
            try{
                tic = System.currentTimeMillis();
                isSuccessful = labelTweetsByCompanyAndArea.mapReduceJob.run();
                toc = System.currentTimeMillis();
            }catch (Exception unknownException){
                logger.error("Unknown Exception while running MapReduce Job", unknownException);
                System.exit(1);
            }
        }

        String result = isSuccessful ? "Successful" : "Failure";
        logger.info(result);
        logger.info("time taken : " + (toc - tic)/1000 + " seconds");
    }

}

