package com.edge.twitter_research.relevance_filter;


import com.edge.twitter_research.core.GlobalConstants;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiURI;
import org.kiji.schema.filter.ColumnValueEqualsRowFilter;
import org.kiji.schema.filter.Filters;
import org.kiji.schema.filter.KijiRowFilter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class TweetToCSVConverter extends Configured {

    public ArrayList<KijiMapReduceJob> mapReduceJobs = null;

    public static Logger logger =
            Logger.getLogger(TweetToCSVConverter.class);

    public TweetToCSVConverter (String rootFilePath,
                       String inputTableName,
                       float samplingRate,
                       String type){

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        try{

            mapReduceJobs = new ArrayList<KijiMapReduceJob>();

            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));

            hBaseConfiguration.setFloat("sampling.rate", samplingRate);
            hBaseConfiguration.set("mapred.textoutputformat.separator", ",");
            //hBaseConfiguration.setInt("hbase.client.scanner.caching", 1000);

            KijiURI tableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", inputTableName)).build();


            KijiRowFilter filter;

            if (!type.equals("relevant")){

                filter =  new ColumnValueEqualsRowFilter(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME,
                        new DecodedCell<String>(Schema.create(Schema.Type.NULL),
                                null));

            }else{
                filter =  new ColumnValueEqualsRowFilter(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME,
                        new DecodedCell<String>(Schema.create(Schema.Type.STRING),
                                GlobalConstants.RELEVANT_RELEVANCE_LABEL));
            }


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


            mapReduceJobs.add(KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(TweetToCSVGatherer.class)
                    .withInputTable(tableUri)
                    .withFilter(filter)
                    .withOutput(new TextMapReduceJobOutput(new Path(rootFilePath +
                            "/result/" + inputTableName + "_" + type + "_" + "regular" + "_" + samplingRate), 1))
                    .addJarDirectory(new Path(additionalJarsPath))
                    .build());

            mapReduceJobs.add(KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(TweetToCSVURLEmphasizer.class)
                    .withInputTable(tableUri)
                    .withFilter(filter)
                    .withOutput(new TextMapReduceJobOutput(new Path(rootFilePath +
                            "/result/" + inputTableName + "_" + type + "_" + "URL" + "_" + samplingRate), 1))
                    .addJarDirectory(new Path(additionalJarsPath))
                    .build());

            mapReduceJobs.add(KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(TweetToCSVMentionEmphasizer.class)
                    .withInputTable(tableUri)
                    .withFilter(filter)
                    .withOutput(new TextMapReduceJobOutput(new Path(rootFilePath +
                            "/result/" + inputTableName + "_" + type + "_" + "mention" + "_" + samplingRate), 1))
                    .addJarDirectory(new Path(additionalJarsPath))
                    .build());

            mapReduceJobs.add(KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(TweetToCSVHashtagEmphasizer.class)
                    .withInputTable(tableUri)
                    .withFilter(filter)
                    .withOutput(new TextMapReduceJobOutput(new Path(rootFilePath +
                            "/result/" + inputTableName + "_" + type + "_" + "hash" + "_" + samplingRate), 1))
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

        if (args.length < 4){
            System.out.println("Usage: TweetToCSVConverter " +
                    "<input_table_name> " +
                    "<HDFS_job_root_file_path> " +
                    "<sampling_rate (%)> " +
                    "<type>");
            return;
        }

        String inputTableName = args[0];
        String HDFSjobRootFilePath = args[1];
        float samplingRate = Float.parseFloat(args[2]);
        String type = args[3];

        TweetToCSVConverter tweetToCSVConverter =
                new TweetToCSVConverter(HDFSjobRootFilePath,
                        inputTableName,
                        samplingRate,
                        type);

        boolean isSuccessful = false;

        for (KijiMapReduceJob mapReduceJob : tweetToCSVConverter.mapReduceJobs){

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
