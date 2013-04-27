package com.edge.twitter_research.relevance_filter;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.KijiMapReduceJobBuilder;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.input.AvroKeyValueMapReduceJobInput;
import org.kiji.mapreduce.output.AvroKeyValueMapReduceJobOutput;


import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.schema.*;

import java.io.IOException;
import java.util.ArrayList;


public class TopSourcesCalculator extends Configured{


    public ArrayList<KijiMapReduceJob> mapReduceJobs = null;

    public static Logger logger =
            Logger.getLogger(TopSourcesCalculator.class);


    public TopSourcesCalculator (String rootFilePath,
                                    String inputTableName,
                                    int splits){

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);
        mapReduceJobs = new ArrayList<KijiMapReduceJob>();

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));
            hBaseConfiguration.setInt("hbase.client.scanner.caching", 1000);

            KijiURI tableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", inputTableName)).build();
            Path intermediatePath = new Path(rootFilePath + "/intermediate/" + inputTableName);
            Path resultPath = new Path(rootFilePath + "/result" + inputTableName);


            mapReduceJobs.add(KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(SourcesGatherer.class)
                    .withReducer(SourcesCounter.class)
                    .withInputTable(tableUri)
                    .withOutput(new AvroKeyValueMapReduceJobOutput(intermediatePath, splits))
                    .build());


            mapReduceJobs.add(KijiMapReduceJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withMapper(CountedSourcesMapper.class)
                    .withReducer(CountedSourcesSorter.class)
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

        if (args.length < 3){
            System.out.println("Usage: TopSourcesCalculator " +
                    "<input_table_name> " +
                    "<HDFS_job_root_file_path> " +
                    "<splits>");
            return;
        }

        String inputTableName = args[0];
        String HDFSjobRootFilePath = args[1];
        int splits = Integer.parseInt(args[2]);

        TopSourcesCalculator topSourcesCalculator =
                new TopSourcesCalculator(HDFSjobRootFilePath,
                        inputTableName,
                        splits);

        boolean isSuccessful = false;

        for (KijiMapReduceJob mapReduceJob : topSourcesCalculator.mapReduceJobs){
            try{
                isSuccessful = mapReduceJob.run();
                if (!isSuccessful)
                    break;
            }catch (Exception unknownException){
                logger.error("Unknown Exception while running MapReduce Job", unknownException);
                System.exit(1);
            }
        }

        String result = isSuccessful ? "Successful" : "Failure";
        logger.info(result);
    }
}
