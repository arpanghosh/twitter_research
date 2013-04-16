package com.edge.twitter_research.relevance_filter;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.HConstants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import org.kiji.mapreduce.KijiMapReduceJobBuilder;
import org.kiji.mapreduce.MapReduceJob;

import org.kiji.mapreduce.gather.KijiGatherJobBuilder;

import org.kiji.mapreduce.input.AvroKeyValueMapReduceJobInput;
import org.kiji.mapreduce.input.TextMapReduceJobInput;
import org.kiji.mapreduce.output.AvroKeyValueMapReduceJobOutput;
import org.kiji.mapreduce.output.TextMapReduceJobOutput;

import org.kiji.schema.*;
import org.kiji.schema.tools.PutTool;

import java.io.IOException;
import java.util.ArrayList;


public class TopEmoticonsCalculator extends Configured{

    public ArrayList<MapReduceJob> mapReduceJobs = null;

    public static Logger logger =
            Logger.getLogger(TopEmoticonsCalculator.class);


    public TopEmoticonsCalculator (String intermediateFilePath,
                                    String inputFilePath,
                                    String resultFilePath,
                                    int splits){

        PropertyConfigurator.configure(this.getClass()
                .getResourceAsStream(Constants.LOG4J_PROPERTIES_FILE_PATH));

        mapReduceJobs = new ArrayList<MapReduceJob>();

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));
            hBaseConfiguration.setInt("hbase.client.scanner.caching", 1000);

            setConf(hBaseConfiguration);

            Path inputPath = new Path(inputFilePath);
            Path intermediatePath = new Path(intermediateFilePath);
            Path resultPath = new Path(resultFilePath);

            mapReduceJobs.add(KijiMapReduceJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withMapper(EmoticonsMapper.class)
                    .withReducer(EmoticonsCounter.class)
                    .withInput(new TextMapReduceJobInput(inputPath))
                    .withOutput(new AvroKeyValueMapReduceJobOutput(intermediatePath, splits))
                    .build());

            mapReduceJobs.add(KijiMapReduceJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withMapper(CountedEmoticonsMapper.class)
                    .withReducer(CountedEmoticonsSorter.class)
                    .withInput(new AvroKeyValueMapReduceJobInput(intermediatePath))
                    .withOutput(new TextMapReduceJobOutput(resultPath, splits))
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

        if (args.length < 4){
            System.out.println("Usage: TopEmoticonsCalculator " +
                    "<HDFS_input_file_path> " +
                    "<HDFS_output_file_path> " +
                    "<HDFS_intermediate_file_path> " +
                    "<splits>");
            return;
        }

        String HDFSInputFilePath = args[0];
        String HDFSOutputFilePath = args[1];
        String HDFSIntermediateFilePath = args[2];
        int splits = Integer.parseInt(args[3]);

        TopEmoticonsCalculator topEmoticonsCalculator =
                new TopEmoticonsCalculator(HDFSIntermediateFilePath,
                        HDFSInputFilePath,
                        HDFSOutputFilePath,
                        splits);

        boolean isSuccessful = false;

        for (MapReduceJob mapReduceJob : topEmoticonsCalculator.mapReduceJobs){

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
