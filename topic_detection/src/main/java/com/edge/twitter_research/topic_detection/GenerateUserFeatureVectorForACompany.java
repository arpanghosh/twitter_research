package com.edge.twitter_research.topic_detection;


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

public class GenerateUserFeatureVectorForACompany extends Configured {


    public KijiMapReduceJob mapReduceJob = null;

    public static Logger logger =
            Logger.getLogger(GenerateUserFeatureVectorForACompany.class);


    public GenerateUserFeatureVectorForACompany (String tableName,
                                                 String companyName,
                                                    String resultFilePath){

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));
            hBaseConfiguration.set("company.name", companyName);
            hBaseConfiguration.set("mapred.textoutputformat.separator", "|");
            hBaseConfiguration.setInt("hbase.client.scanner.caching", 1000);

            KijiURI tableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", tableName)).build();
            Path resultPath = new Path(resultFilePath);


            mapReduceJob = KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(UserToFeatureVectorGatherer.class)
                    .withReducer(UserFeatureVectorUniquer.class)
                    .withInputTable(tableUri)
                    .withOutput(new TextMapReduceJobOutput(resultPath, 1))
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
            System.out.println("Usage: GenerateUserFeatureVectorForACompany " +
                    "<input_table_name> " +
                    "<company_name> " +
                    "<HDFS_result_file_path>");
            return;
        }

        String inputTableName = args[0];
        String companyName = args[1];
        String HDFSResultFilePath = args[2];

        GenerateUserFeatureVectorForACompany generateUserFeatureVectorForACompany =
                new GenerateUserFeatureVectorForACompany(inputTableName,
                                                        companyName,
                                                        HDFSResultFilePath);

        boolean isSuccessful = false;

        try{
            isSuccessful = generateUserFeatureVectorForACompany.mapReduceJob.run();
        }catch (Exception unknownException){
            logger.error("Unknown Exception while running MapReduce Job", unknownException);
            System.exit(1);
        }


        String result = isSuccessful ? "Successful" : "Failure";
        logger.info(result);
    }
}

