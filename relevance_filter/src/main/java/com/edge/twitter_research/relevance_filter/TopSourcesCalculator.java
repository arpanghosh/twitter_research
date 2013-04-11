package com.edge.twitter_research.relevance_filter;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import org.kiji.mapreduce.KijiMapReduceJobBuilder;
import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.input.AvroKeyValueMapReduceJobInput;
import org.kiji.mapreduce.output.AvroKeyValueMapReduceJobOutput;


import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.schema.*;

import java.io.IOException;
import java.util.ArrayList;


public class TopSourcesCalculator extends Configured{


    public ArrayList<MapReduceJob> mapReduceJobs = null;

    public static Logger logger =
            Logger.getLogger(TopSourcesCalculator.class);


    public TopSourcesCalculator (String intermediateFilePath,
                                    String inputTableName,
                                    String resultFilePath,
                                    int splits){

        PropertyConfigurator.configure(this.getClass().getResourceAsStream(Constants.LOG4J_PROPERTIES_FILE_PATH));
        mapReduceJobs = new ArrayList<MapReduceJob>();

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));
            //hBaseConfiguration.setInt("hbase.client.scanner.caching", 500);

            KijiURI tableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", inputTableName)).build();
            Path intermediatePath = new Path(intermediateFilePath);
            Path resultPath = new Path(resultFilePath);


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

        if (args.length < 4){
            System.out.println("Usage: TopSourcesCalculator " +
                    "<input_table_name> " +
                    "<HDFS_intermediate_file_path> " +
                    "<HDFS_result_file_path> " +
                    "<splits>");
            return;
        }

        String inputTableName = args[0];
        String HDFSIntermediateFilePath = args[1];
        String HDFSResultFilePath = args[2];
        int splits = Integer.parseInt(args[3]);

        TopSourcesCalculator topSourcesCalculator =
                new TopSourcesCalculator(HDFSIntermediateFilePath,
                        inputTableName,
                        HDFSResultFilePath,
                        splits);

        boolean isSuccessful = false;

        for (MapReduceJob mapReduceJob : topSourcesCalculator.mapReduceJobs){
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
