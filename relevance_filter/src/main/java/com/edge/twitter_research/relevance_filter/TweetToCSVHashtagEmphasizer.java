package com.edge.twitter_research.relevance_filter;


import com.edge.twitter_research.core.GlobalConstants;
import com.edge.twitter_research.core.SimpleHashtagEntity;
import com.edge.twitter_research.core.SimpleTweet;
import com.edge.twitter_research.core.SimpleURLEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

import java.io.IOException;

public class TweetToCSVHashtagEmphasizer
        extends KijiGatherer<LongWritable, Text> {

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

            SimpleTweet tweet = input.getMostRecentValue(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                    GlobalConstants.TWEET_COLUMN_NAME);


            String category = input.getEntityId().getComponentByIndex(0);

            StringBuilder tweetText = new StringBuilder();
            tweetText.append(tweet.getText());
            tweetText.append(" ");
            for (SimpleHashtagEntity hashtagEntity : tweet.getHashTagEntities()){
                tweetText.append(hashtagEntity.getText());
                tweetText.append(" ");
                tweetText.append(hashtagEntity.getText());
                tweetText.append(" ");
                tweetText.append(hashtagEntity.getText());
                tweetText.append(" ");
                tweetText.append(hashtagEntity.getText());
                tweetText.append(" ");
            }


            char[] tweetTextAsCharArray = tweetText.toString().toCharArray();

            for (int i = 0; i < tweetTextAsCharArray.length - 1; i++){
                if (!Character.isLetterOrDigit(tweetTextAsCharArray[i]))
                    tweetTextAsCharArray[i] = ' ';
            }

            String cleanTweetText = new String(tweetTextAsCharArray);

            context.write(new LongWritable(tweet.getId()),
                    new Text("\"" + cleanTweetText + "\"," + category));

        }
    }


    @Override
    public KijiDataRequest getDataRequest() {
        // This method is how we specify which columns in each row the gatherer operates on.
        // In this case, we need all versions of the tweet_object:tweet column.
        final KijiDataRequestBuilder builder = KijiDataRequest.builder();
        builder.newColumnsDef()
                .withMaxVersions(1)
                .add(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.TWEET_COLUMN_NAME);
        return builder.build();
    }


    @Override
    public Class<?> getOutputValueClass() {
        return Text.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return LongWritable.class;
    }


}

