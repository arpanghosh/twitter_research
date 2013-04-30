package com.edge.twitter_research.topic_detection;

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
import org.kiji.mapreduce.input.AvroKeyValueMapReduceJobInput;
import org.kiji.mapreduce.output.TextMapReduceJobOutput;


import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;


public class CompanyUserListToCSV extends Configured {


    public KijiMapReduceJob mapReduceJob;

    public static Logger logger =
            Logger.getLogger(CompanyUserListToCSV.class);


    public CompanyUserListToCSV (String companyName,
                                String rootPath,
                                String threshold){

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));
            hBaseConfiguration.set("mapred.textoutputformat.separator", ",");
            hBaseConfiguration.set("company.name", companyName);
            //hBaseConfiguration.setInt("hbase.client.scanner.caching", 1000);

            Path inputPath = new Path(rootPath + "/result_" + threshold);
            Path resultPath = new Path(rootPath + "/CSV/" + companyName);

            /*
            FileSystem fs = FileSystem.get(hBaseConfiguration);
            if (!fs.exists(resultPath)){
                fs.mkdirs(resultPath);
            }
            */

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

            mapReduceJob =  KijiMapReduceJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withMapper(CompanyUserListToCSVMapper.class)
                    .withInput(new AvroKeyValueMapReduceJobInput(inputPath))
                    .withOutput(new TextMapReduceJobOutput(resultPath, 1))
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
            System.out.println("Usage: CompanyUserListToCSV " +
                    "<company_name> " +
                    "<HDFS_job_root_path> " +
                    "<threshold>");
            return;
        }

        String companyName = args[0];
        String HDFSrootPath = args[1];
        String threshold = args[2];

        CompanyUserListToCSV companyUserListToCSV =
                new CompanyUserListToCSV(companyName,
                                        HDFSrootPath,
                                        threshold);

        boolean isSuccessful = false;

        try{
            isSuccessful = companyUserListToCSV.mapReduceJob.run();
        }catch (Exception unknownException){
            logger.error("Unknown Exception while running MapReduce Job", unknownException);
            System.exit(1);
        }

        String result = isSuccessful ? "Successful" : "Failure";
        logger.info(result);
    }
}
