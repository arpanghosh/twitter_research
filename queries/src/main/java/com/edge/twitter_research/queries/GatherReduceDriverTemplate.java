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

public class GatherReduceDriverTemplate extends Configured {

    public KijiMapReduceJob mapReduceJob = null;

    public static Logger logger =
            Logger.getLogger(GatherReduceDriverTemplate.class);

    public GatherReduceDriverTemplate (String outputFilePath,
                                        String inputTableName){

        PropertyConfigurator.configure(this.getClass()
                .getResourceAsStream(Constants.LOG4J_PROPERTIES_FILE_PATH));

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));

            hBaseConfiguration.set("mapred.textoutputformat.separator", "|");
            hBaseConfiguration.setInt("hbase.client.scanner.caching", 1000);

            KijiURI tableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", inputTableName)).build();

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

            this.mapReduceJob = KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(GathererTemplate.class)
                    .withReducer(ReducerTemplate.class)
                    .withInputTable(tableUri)
                    .withOutput(new TextMapReduceJobOutput(new Path(outputFilePath), 1))
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

        if (args.length < 2){
            System.out.println("Usage: GatherReduceDriverTemplate " +
                    "<input_table_name> " +
                    "<HDFS_output_file_path>");
            return;
        }


        String inputTableName = args[0];
        String HDFSOutputFilePath = args[1];


        GatherReduceDriverTemplate gatherReduceDriverTemplate =
                                new GatherReduceDriverTemplate(HDFSOutputFilePath,
                                                                inputTableName);


        boolean isSuccessful = false;
        if (gatherReduceDriverTemplate.mapReduceJob != null){
            try{
                isSuccessful = gatherReduceDriverTemplate.mapReduceJob.run();
            }catch (Exception unknownException){
                logger.error("Unknown Exception while running MapReduce Job", unknownException);
                System.exit(1);
            }
        }

        String result = isSuccessful ? "Successful" : "Failure";
        logger.info(result);
    }

}

