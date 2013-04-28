package com.edge.twitter_research.relevance_filter;


import com.edge.twitter_research.core.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.KijiMapReduceJobBuilder;
import org.kiji.mapreduce.input.TextMapReduceJobInput;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.schema.KijiURI;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class SimpleRelevanceLabelImporter extends Configured {

    public KijiMapReduceJob mapReduceJob = null;

    public static Logger logger =
            Logger.getLogger(SimpleRelevanceLabelImporter.class);

    public SimpleRelevanceLabelImporter (String rootFilePath,
                                          String outputTableName,
                                          int numReducers){

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));


            KijiURI tableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", outputTableName)).build();

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

            this.mapReduceJob = KijiMapReduceJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withInput(new TextMapReduceJobInput(new Path(rootFilePath + "/input/" + outputTableName)))
                    .withMapper(IDLabelMapper.class)
                    .withReducer(RelevanceLabelWriter.class)
                    .withOutput(new DirectKijiTableMapReduceJobOutput(tableUri, numReducers))
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

        if (args.length < 3){
            System.out.println("Usage: SimpleRelevanceLabelImporter " +
                    "<HDFS_job_root_file_path> " +
                    "<output_table_name> " +
                    "<num_reducers>");
            return;
        }

        String HDFSjobRootFilePath = args[0];
        String outputTableName = args[1];
        int numReducers = Integer.parseInt(args[2]);


        SimpleRelevanceLabelImporter simpleRelevanceLabelImporter =
                new SimpleRelevanceLabelImporter(HDFSjobRootFilePath,
                                                outputTableName,
                                                numReducers);

        boolean isSuccessful = false;
        if (simpleRelevanceLabelImporter.mapReduceJob != null){
            try{
                isSuccessful = simpleRelevanceLabelImporter.mapReduceJob.run();
            }catch (Exception unknownException){
                logger.error("Unknown Exception while running MapReduce Job", unknownException);
                System.exit(1);
            }
        }

        String result = isSuccessful ? "Successful" : "Failure";
        logger.info(result);
    }

}

