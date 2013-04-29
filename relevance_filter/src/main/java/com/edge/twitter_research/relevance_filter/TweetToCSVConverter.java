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

public class TweetToCSVConverter extends Configured {

    public KijiMapReduceJob mapReduceJob = null;

    public static Logger logger =
            Logger.getLogger(TweetToCSVConverter.class);

    public TweetToCSVConverter (String rootFilePath,
                       String inputTableName,
                       float samplingRate,
                       String type){

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));

            hBaseConfiguration.setFloat("sampling.rate", samplingRate);
            hBaseConfiguration.set("mapred.textoutputformat.separator", ",");
            //hBaseConfiguration.setInt("hbase.client.scanner.caching", 1000);

            KijiURI tableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", inputTableName)).build();


            KijiRowFilter filter;

            if (!type.equals("relevant")){

                ColumnValueEqualsRowFilter filter1 =  new ColumnValueEqualsRowFilter(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME,
                        new DecodedCell<String>(Schema.create(Schema.Type.STRING),
                                GlobalConstants.NOT_RELEVANT_RELEVANCE_LABEL));

                ColumnValueEqualsRowFilter filter2 =  new ColumnValueEqualsRowFilter(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME,
                        new DecodedCell<String>(Schema.create(Schema.Type.NULL),
                                null));

                filter = Filters.or(filter1, filter2);

            }else{
                filter =  new ColumnValueEqualsRowFilter(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME,
                        new DecodedCell<String>(Schema.create(Schema.Type.STRING),
                                GlobalConstants.RELEVANT_RELEVANCE_LABEL));
            }


            this.mapReduceJob = KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(TweetToCSVGatherer.class)
                    .withInputTable(tableUri)
                    .withFilter(filter)
                    .withOutput(new TextMapReduceJobOutput(new Path(rootFilePath + "/result/" + inputTableName + "/" + type + "/" + samplingRate), 1))
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

        if (args.length < 4){
            System.out.println("Usage: TweetToCSVConverter " +
                    "<input_table_name> " +
                    "<HDFS_job_root_file_path> " +
                    "<sampling_rate (%)> " +
                    "type");
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
        if (tweetToCSVConverter.mapReduceJob != null){
            try{
                isSuccessful = tweetToCSVConverter.mapReduceJob.run();
            }catch (Exception unknownException){
                logger.error("Unknown Exception while running MapReduce Job", unknownException);
                System.exit(1);
            }
        }

        String result = isSuccessful ? "Successful" : "Failure";
        logger.info(result);
    }

}
