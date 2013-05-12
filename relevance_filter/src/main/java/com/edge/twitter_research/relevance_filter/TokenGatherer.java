package com.edge.twitter_research.relevance_filter;

import cmu.arktweetnlp.Twokenize;
import com.edge.twitter_research.core.GlobalConstants;
import com.edge.twitter_research.core.SimpleTweet;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

import java.io.IOException;
import java.util.List;


public class TokenGatherer
        extends KijiGatherer<Text, Text> {


    @Override
    public void gather(KijiRowData input, GathererContext<Text, Text> context)
            throws IOException {

        Utf8 relevanceLabel = input.getMostRecentValue(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME);

        if (relevanceLabel != null){
            SimpleTweet tweet = input.getMostRecentValue(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                    GlobalConstants.TWEET_COLUMN_NAME);

            List<String> tokens = Twokenize.tokenizeRawTweetText(tweet.getText().toString());

            for (String token : tokens){
                context.write(new Text(relevanceLabel.toString()), new Text(token));
            }

        }
    }


    @Override
    public KijiDataRequest getDataRequest() {
        // This method is how we specify which columns in each row the gatherer operates on.
        // In this case, we need all versions of the tweet_object:tweet column.
        final KijiDataRequestBuilder builder = KijiDataRequest.builder();
        builder.newColumnsDef()
                .withMaxVersions(1)
                .addFamily(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME);
        return builder.build();
    }


    @Override
    public Class<?> getOutputValueClass() {
        return Text.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return Text.class;
    }


}
