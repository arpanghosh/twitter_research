package com.edge.twitter_research.relevance_filter;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.KijiMapReduceJobBuilder;


import org.kiji.mapreduce.input.AvroKeyValueMapReduceJobInput;
import org.kiji.mapreduce.input.TextMapReduceJobInput;
import org.kiji.mapreduce.output.AvroKeyValueMapReduceJobOutput;
import org.kiji.mapreduce.output.TextMapReduceJobOutput;


import java.io.IOException;
import java.util.ArrayList;


public class TopEmoticonsCalculator extends Configured{

    public ArrayList<KijiMapReduceJob> mapReduceJobs = null;

    public static Logger logger =
            Logger.getLogger(TopEmoticonsCalculator.class);


    public TopEmoticonsCalculator (String rootFilePath,
                                    int splits){

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        mapReduceJobs = new ArrayList<KijiMapReduceJob>();

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));
            hBaseConfiguration.setInt("hbase.client.scanner.caching", 1000);

            setConf(hBaseConfiguration);

            Path inputPath = new Path(rootFilePath + "/input");
            Path intermediatePath = new Path(rootFilePath + "/intermediate");
            Path resultPath = new Path(rootFilePath + "/result");

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

        if (args.length < 2){
            System.out.println("Usage: TopEmoticonsCalculator " +
                    "<HDFS_job_root_file_path> " +
                    "<splits>");
            return;
        }

        String HDFSjobRootFilePath = args[0];
        int splits = Integer.parseInt(args[1]);

        TopEmoticonsCalculator topEmoticonsCalculator =
                new TopEmoticonsCalculator(HDFSjobRootFilePath,
                        splits);

        boolean isSuccessful = false;

        for (KijiMapReduceJob mapReduceJob : topEmoticonsCalculator.mapReduceJobs){

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
