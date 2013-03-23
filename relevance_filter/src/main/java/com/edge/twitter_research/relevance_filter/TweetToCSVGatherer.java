package com.edge.twitter_research.relevance_filter;


import com.edge.twitter_research.core.SimpleTweet;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueWriter;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

import java.io.IOException;

public class TweetToCSVGatherer
        extends KijiGatherer<LongWritable, CharSequence> {

    private static final String TWEET_COLUMN_FAMILY = "tweet_object";
    private static final String TWEET_COLUMN = "tweet";

    @Override
    public void gather(KijiRowData input, GathererContext<LongWritable, CharSequence> context)
            throws IOException {

        Long tweetID = input.getEntityId()
                            .getComponentByIndex(TweetToCSV.tweetIDInKeyIndex);

        SimpleTweet tweet = input.getMostRecentValue(TWEET_COLUMN_FAMILY, TWEET_COLUMN);
        String tweetTextWithoutCommas = tweet.getText()
                                        .toString()
                                        .replace(",", "<comma>");

        context.write(new LongWritable(tweetID),
                tweetTextWithoutCommas);
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

