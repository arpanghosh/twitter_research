package com.edge.twitter_research.relevance_filter;


import com.edge.twitter_research.core.SimpleTweet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

import java.io.IOException;

public class TweetToCSVGatherer
        extends KijiGatherer<LongWritable, Text> {

    private static final String TWEET_COLUMN_FAMILY = "tweet_object";
    private static final String TWEET_COLUMN = "tweet";

    private double threshold;
    private int tweetIdIndex;

    @Override
    public void setup(GathererContext<LongWritable, Text> context) throws IOException {
        super.setup(context); // Any time you override setup, call super.setup(context);

        Configuration conf = getConf();
        threshold = conf.getFloat("sampling.rate", 100)/100.0;
        tweetIdIndex = conf.getInt("key.index.of.tweet_id", 0);
    }


    @Override
    public void gather(KijiRowData input, GathererContext<LongWritable, Text> context)
            throws IOException {

        Long tweetID = input.getEntityId()
                            .getComponentByIndex(tweetIdIndex);

        SimpleTweet tweet = input.getMostRecentValue(TWEET_COLUMN_FAMILY, TWEET_COLUMN);
        String tweetTextWithoutCommas = tweet.getText()
                                        .toString()
                                        .replace(",", "<comma>");

        if (Math.random() < threshold)
            context.write(new LongWritable(tweetID),
                            new Text(tweetTextWithoutCommas));

    }


    @Override
    public KijiDataRequest getDataRequest() {
        // This method is how we specify which columns in each row the gatherer operates on.
        // In this case, we need all versions of the tweet_object:tweet column.
        final KijiDataRequestBuilder builder = KijiDataRequest.builder();
        builder.newColumnsDef()
                .withMaxVersions(1)
                .add(TWEET_COLUMN_FAMILY, TWEET_COLUMN);
        return builder.build();
    }


    @Override
    public Class<?> getOutputValueClass() {
        return CharSequence.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return LongWritable.class;
    }


}

