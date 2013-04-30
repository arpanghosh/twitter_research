package com.edge.twitter_research.relevance_filter;


import com.edge.twitter_research.core.GlobalConstants;
import org.apache.avro.Schema;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.filter.ColumnValueEqualsRowFilter;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.filter.OrRowFilter;
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
import org.kiji.schema.filter.Filters;

import java.io.IOException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class TweetToFeatureVectorConverter extends Configured {

    public ArrayList<KijiMapReduceJob> mapReduceJobs = null;

    public static Logger logger =
            Logger.getLogger(TweetToFeatureVectorConverter.class);

    public TweetToFeatureVectorConverter (String rootFilePath,
                       String inputTableName,
                       float samplingRate,
                       String dataSetType){

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        try{
            mapReduceJobs = new ArrayList<KijiMapReduceJob>();

            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));

            hBaseConfiguration.setFloat("sampling.rate", samplingRate);
            hBaseConfiguration.set("mapred.textoutputformat.separator", ",");
            //hBaseConfiguration.set("feature.type", featureType);
            //hBaseConfiguration.setInt("hbase.client.scanner.caching", 1000);
            hBaseConfiguration.setBoolean("generating.training.set", dataSetType.equals("training"));

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

            KijiRowFilter filter;

            if (dataSetType.equals("training")){

                ColumnValueEqualsRowFilter filter1 =  new ColumnValueEqualsRowFilter(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                                GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME,
                                new DecodedCell<String>(Schema.create(Schema.Type.STRING),
                                        GlobalConstants.RELEVANT_RELEVANCE_LABEL));

                ColumnValueEqualsRowFilter filter2 =  new ColumnValueEqualsRowFilter(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                                GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME,
                                new DecodedCell<String>(Schema.create(Schema.Type.STRING),
                                        GlobalConstants.NOT_RELEVANT_RELEVANCE_LABEL));

                filter = Filters.or(filter1, filter2);

            }else{
                filter =  new ColumnValueEqualsRowFilter(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME,
                        new DecodedCell<String>(Schema.create(Schema.Type.NULL),
                                null));
            }


            mapReduceJobs.add(KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(TweetToFeatureVectorGathererTweet.class)
                    .withInputTable(tableUri)
                    .withFilter(filter)
                    .withOutput(new TextMapReduceJobOutput(new Path(rootFilePath +
                            "/result/" + inputTableName + "_" + "tweet"), 1))
                    .addJarDirectory(new Path(additionalJarsPath))
                    .build());

            mapReduceJobs.add(KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(TweetToFeatureVectorGathererLanguage.class)
                    .withInputTable(tableUri)
                    .withFilter(filter)
                    .withOutput(new TextMapReduceJobOutput(new Path(rootFilePath +
                            "/result/" + inputTableName + "_" + "language"), 1))
                    .addJarDirectory(new Path(additionalJarsPath))
                    .build());

            mapReduceJobs.add(KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(TweetToFeatureVectorGathererUser.class)
                    .withInputTable(tableUri)
                    .withFilter(filter)
                    .withOutput(new TextMapReduceJobOutput(new Path(rootFilePath +
                            "/result/" + inputTableName + "_" + "user"), 1))
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
            System.out.println("Usage: TweetToFeatureVectorConverter " +
                    "<input_table_name> " +
                    "<HDFS_job_root_file_path> " +
                    "<data_type (testing or training)> " +
                    "<sampling_rate (%)>");
            return;
        }

        String dataSet = args[2];
        if (!dataSet.equals("training") && !dataSet.equals("testing")){
            System.out.println("Enter a valid dataset type");
            return;
        }

        String inputTableName = args[0];
        String HDFSjobRootFilePath = args[1];
        float samplingRate;
        if (dataSet.equals("training"))
            samplingRate = 100;
        else if (args.length > 3)
            samplingRate = Float.parseFloat(args[3]);
        else
            samplingRate = 50;

        TweetToFeatureVectorConverter tweetToFeatureVector =
                new TweetToFeatureVectorConverter(HDFSjobRootFilePath,
                        inputTableName,
                        samplingRate,
                        dataSet);

        boolean isSuccessful = false;

        for (KijiMapReduceJob mapReduceJob : tweetToFeatureVector.mapReduceJobs){

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
