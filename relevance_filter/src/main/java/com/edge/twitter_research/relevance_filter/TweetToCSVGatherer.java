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

    @Override
    public void setup(GathererContext<LongWritable, Text> context) throws IOException {
        super.setup(context); // Any time you override setup, call super.setup(context);

        Configuration conf = getConf();
        threshold = conf.getFloat("sampling.rate", 100)/100.0;
    }


    @Override
    public void gather(KijiRowData input, GathererContext<LongWritable, Text> context)
            throws IOException {


        if (Math.random() < threshold){

            SimpleTweet tweet = input.getMostRecentValue(TWEET_COLUMN_FAMILY, TWEET_COLUMN);

            char[] tweetTextCharArray = tweet.getText().toString()
                                        .replace(",", "<comma>")
                                        .toCharArray();
            for (int i = 0; i < tweetTextCharArray.length; i++){
                if (!Character.isLetterOrDigit(tweetTextCharArray[i])){
                    tweetTextCharArray[i] = ' ';
                }
            }

            String tweetTextWithoutCommasAndNonSupplementaryCharacters
                    = new String (tweetTextCharArray);

            context.write(new LongWritable(tweet.getId()),
                            new Text(tweetTextWithoutCommasAndNonSupplementaryCharacters));
        }
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

