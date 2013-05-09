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
import org.kiji.schema.filter.HasColumnDataRowFilter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;



public class PerTimeVolume extends Configured {

    public ArrayList<KijiMapReduceJob> mapReduceJobs;

    public static Logger logger =
            Logger.getLogger(PerTimeVolume.class);

    public PerTimeVolume (String rootFilePath,
                                              String inputTableName,
                                              String granularity,
                                              int numReducers){

        mapReduceJobs = new ArrayList<KijiMapReduceJob>();

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));
            hBaseConfiguration.set("granularity", granularity);

            hBaseConfiguration.set("mapred.textoutputformat.separator", ",");
            //hBaseConfiguration.setInt("hbase.client.scanner.caching", 1000);

            Path resultFilePath = new Path(rootFilePath + "/result/" + granularity );

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




            HasColumnDataRowFilter filter =
                    new HasColumnDataRowFilter(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                            GlobalConstants.COMPANY_DATA_COLUMN_NAME);

            mapReduceJobs.add(KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(TimeGatherer.class)
                    .withFilter(filter)
                    .withReducer(PerTimeCounter.class)
                    .withInputTable(tableUri)
                    .withOutput(new TextMapReduceJobOutput(resultFilePath, numReducers))
                    .addJarDirectory(new Path(additionalJarsPath))
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
            System.out.println("Usage: PerTimeVolume " +
                    "<HDFS_job_root_file_path> " +
                    "<granularity> " +
                    "<num_reducers>");
            return;
        }


        String inputTableName = "filter_tweet_store";
        String HDFSjobRootFilePath = args[0];
        String granularity = args[1];
        int numReducers = Integer.parseInt(args[2]);


        PerTimeVolume perTimeVolume =
                new PerTimeVolume(HDFSjobRootFilePath,
                        inputTableName,
                        granularity,
                        numReducers);


        boolean isSuccessful = false;

        for (KijiMapReduceJob mapReduceJob : perTimeVolume.mapReduceJobs){

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
