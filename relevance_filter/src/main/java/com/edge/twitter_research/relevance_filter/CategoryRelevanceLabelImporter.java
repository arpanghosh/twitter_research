package com.edge.twitter_research.relevance_filter;

import com.edge.twitter_research.core.GlobalConstants;
import com.edge.twitter_research.core.KijiConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder;
import org.kiji.mapreduce.input.TextMapReduceJobInput;
import org.kiji.mapreduce.kvstore.lib.KijiTableKeyValueStore;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.mapreduce.produce.KijiProduceJobBuilder;
import org.kiji.schema.KijiURI;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;


public class CategoryRelevanceLabelImporter
    extends Configured {


    public ArrayList<KijiMapReduceJob> mapReduceJobs;

    public static Logger logger =
            Logger.getLogger(CategoryRelevanceLabelImporter.class);

    public CategoryRelevanceLabelImporter (String outputTableName,
                                           String tempTableName,
                                          String rootFilePath,
                                          int numReducers){

        mapReduceJobs = new ArrayList<KijiMapReduceJob>();

        PropertyConfigurator.configure(Constants.LOG4J_PROPERTIES_FILE_PATH);

        try{
            Configuration hBaseConfiguration =
                    HBaseConfiguration.addHbaseResources(new Configuration(true));
            hBaseConfiguration.set("temporary.table.name", tempTableName);

            KijiURI outputTableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", outputTableName)).build();
            KijiURI tempTableUri =
                    KijiURI.newBuilder(String.format("kiji://.env/default/%s", tempTableName)).build();

            KijiConnection kijiConnection = new KijiConnection (this.getClass()
                                .getResourceAsStream("/category_relevance_label_temp_table_layout.json"),
                                outputTableName);

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


            mapReduceJobs.add(KijiBulkImportJobBuilder.create()
                    .withConf(hBaseConfiguration)
                    .withBulkImporter(CategoryRelevanceLabelBulkImporter.class)
                    .withInput(new TextMapReduceJobInput(new Path(rootFilePath + "/input/" + outputTableName)))
                    .withOutput(new DirectKijiTableMapReduceJobOutput(tempTableUri, numReducers))
                    .addJarDirectory(new Path(additionalJarsPath))
                    .build());

            KijiTableKeyValueStore.Builder kvStoreBuilder = KijiTableKeyValueStore.builder();
            kvStoreBuilder.withColumn("tweet_relevance_label",
                    GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME).withTable(tempTableUri);

            mapReduceJobs.add(KijiProduceJobBuilder.create()
                                .withConf(hBaseConfiguration)
                                .withProducer(CategoryRelevanceLabelProducer.class)
                                .withStore("relevanceLabels", kvStoreBuilder.build())
                                .withInputTable(outputTableUri)
                                .withOutput(new DirectKijiTableMapReduceJobOutput(outputTableUri, 1))
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
            System.out.println("Usage: CategoryRelevanceLabelImporter " +
                    "<HDFS_job_root_file_path> " +
                    "<output_table_name> " +
                    "<num_reducers>");
            return;
        }


        String HDFSjobRootFilePath = args[0];
        String tempTableName = "category_relevance_label_temp_table";
        String outputTableName = args[1];
        int numReducers = Integer.parseInt(args[2]);

        CategoryRelevanceLabelImporter categoryRelevanceLabelImporter =
                new CategoryRelevanceLabelImporter(outputTableName,
                                                    tempTableName,
                                                    HDFSjobRootFilePath,
                                                    numReducers);

        boolean isSuccessful = false;

        for (KijiMapReduceJob mapReduceJob : categoryRelevanceLabelImporter.mapReduceJobs){
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

