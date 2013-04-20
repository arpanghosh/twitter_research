package com.edge.twitter_research.relevance_filter;

import com.edge.twitter_research.core.GlobalConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;

import java.io.IOException;


public class CategoryRelevanceLabelBulkImporter
    extends KijiBulkImporter<LongWritable, Text>{

    /** {@inheritDoc} */
    @Override
    public void produce(LongWritable filePos, Text line, KijiTableContext context)
            throws IOException {

        String[] tokens = line.toString().split(",");

        String id = tokens[1].replaceAll("^\"|\"$", "");
        String relevanceLabel = tokens[4].replaceAll("^\"|\"$", "");

        context.put(context.getEntityId(id),
                    "tweet_relevance_label",
                    GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME,
                        relevanceLabel);
    }
}


