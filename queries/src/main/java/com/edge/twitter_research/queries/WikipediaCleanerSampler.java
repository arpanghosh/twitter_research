package com.edge.twitter_research.queries;


import com.edge.twitter_research.core.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.KijiMapReduceJobBuilder;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.input.AvroKeyValueMapReduceJobInput;
import org.kiji.mapreduce.input.TextMapReduceJobInput;
import org.kiji.mapreduce.output.AvroKeyValueMapReduceJobOutput;
import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.schema.KijiURI;
import org.kiji.schema.filter.HasColumnDataRowFilter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;



public class WikipediaCleanerSampler extends Configured {

    public KijiMapReduceJob mapReduceJob;

    public static Logger logger =
            Logger.getLogger(WikipediaCleanerSampler.class);

    public WikipediaCleanerSampler (String rootFilePath,
                                              String dataSet,
                                              float samplingRate){


        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));
            hBaseConfiguration.setFloat("sampling.rate", samplingRate);
            hBaseConfiguration.set("mapred.textoutputformat.separator", "|");
            //hBaseConfiguration.setInt("hbase.client.scanner.caching", 1000);

            Path resultFilePath = new Path(rootFilePath + "/result/" + dataSet );
            Path inputFilePath = new Path(rootFilePath + "/input/" + dataSet);


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


            if (dataSet.equals("links")){

                mapReduceJob = KijiMapReduceJobBuilder.create()
                        .withConf(hBaseConfiguration)
                        .withMapper(WikiLinksCleanerSampler.class)
                        .withInput(new TextMapReduceJobInput(inputFilePath))
                        .withOutput(new TextMapReduceJobOutput(resultFilePath, 1))
                        .addJarDirectory(new Path(additionalJarsPath))
                        .build();
            }


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
            System.out.println("Usage: WikipediaCleanerSampler " +
                    "<HDFS_job_root_file_path> " +
                    "<dataset> " +
                    "<sampling_rate>");
            return;
        }


        String HDFSjobRootFilePath = args[0];
        String dataset = args[1];
        float samplingRate = Float.parseFloat(args[2]);


        WikipediaCleanerSampler wikipediaCleanerSampler =
                new WikipediaCleanerSampler(HDFSjobRootFilePath,
                                            dataset,
                                            samplingRate);


        boolean isSuccessful = false;


        if (wikipediaCleanerSampler.mapReduceJob != null){
            try{
                isSuccessful = wikipediaCleanerSampler.mapReduceJob.run();
            }catch (Exception unknownException){
                logger.error("Unknown Exception while running MapReduce Job", unknownException);
                System.exit(1);
            }
        }

        String result = isSuccessful ? "Successful" : "Failure";
        logger.info(result);
    }

}