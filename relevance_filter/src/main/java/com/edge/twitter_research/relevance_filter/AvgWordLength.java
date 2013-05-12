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
import org.kiji.mapreduce.KijiMapReduceJobBuilder;
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



public class AvgWordLength extends Configured {

    public KijiMapReduceJob mapReduceJob = null;

    public static Logger logger =
            Logger.getLogger(AvgWordLength.class);

    public AvgWordLength (String rootFilePath,
                          String inputTableName){

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        try{

            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));

            hBaseConfiguration.set("mapred.textoutputformat.separator", ",");
            //hBaseConfiguration.set("feature.type", featureType);
            //hBaseConfiguration.setInt("hbase.client.scanner.caching", 1000);

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


                ColumnValueEqualsRowFilter filter1 =  new ColumnValueEqualsRowFilter(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME,
                        new DecodedCell<String>(Schema.create(Schema.Type.STRING),
                                GlobalConstants.RELEVANT_RELEVANCE_LABEL));

                ColumnValueEqualsRowFilter filter2 =  new ColumnValueEqualsRowFilter(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME,
                        new DecodedCell<String>(Schema.create(Schema.Type.STRING),
                                GlobalConstants.NOT_RELEVANT_RELEVANCE_LABEL));

                filter = Filters.or(filter1, filter2);




            mapReduceJob = KijiGatherJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withGatherer(TokenGatherer.class)
                    .withReducer(AvgWordLengthCalculator.class)
                    .withInputTable(tableUri)
                    .withFilter(filter)
                    .withOutput(new TextMapReduceJobOutput(new Path(rootFilePath +
                            "/result/" + inputTableName), 1))
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
            System.out.println("Usage: AvgWordLength " +
                    "<input_table_name> " +
                    "<HDFS_job_root_file_path>");
            return;
        }



        String inputTableName = args[0];
        String HDFSjobRootFilePath = args[1];


        AvgWordLength avgWordLength =
                new AvgWordLength(HDFSjobRootFilePath,
                        inputTableName);

        boolean isSuccessful = false;


            if (avgWordLength.mapReduceJob != null){
                try{
                    isSuccessful = avgWordLength.mapReduceJob.run();
                }catch (Exception unknownException){
                    logger.error("Unknown Exception while running MapReduce Job", unknownException);
                    System.exit(1);
                }
            }


        String result = isSuccessful ? "Successful" : "Failure";
        logger.info(result);
    }

}