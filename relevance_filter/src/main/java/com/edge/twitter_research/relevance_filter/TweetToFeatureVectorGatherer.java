package com.edge.twitter_research.relevance_filter;


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

public class TweetToFeatureVectorGatherer
        extends KijiGatherer<LongWritable, Text> {

    private double threshold;
    private TweetFeatureVectorGenerator tweetFeatureVectorGenerator;
    private boolean generatingTrainingSet;
    private String type;

    @Override
    public void setup(GathererContext<LongWritable, Text> context) throws IOException {
        super.setup(context); // Any time you override setup, call super.setup(context);

        Configuration conf = getConf();
        threshold = conf.getFloat("sampling.rate", 100)/100.0;
        generatingTrainingSet = conf.getBoolean("generating.training.set", false);
        type = conf.get("type", "tweet");
        tweetFeatureVectorGenerator = new TweetFeatureVectorGenerator();
    }


    @Override
    public void gather(KijiRowData input, GathererContext<LongWritable, Text> context)
            throws IOException {

        if (Math.random() < threshold){

            SimpleTweet tweet =
                    input.getMostRecentValue(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                                                        GlobalConstants.TWEET_COLUMN_NAME);

            String tweetFeatureVector;

            if (generatingTrainingSet){
                Utf8 relevanceLabel =
                        input.getMostRecentValue(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                                                GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME);
                try{
                    tweetFeatureVector =
                        tweetFeatureVectorGenerator.getFeatureVector(tweet,
                                                    relevanceLabel.toString());
                }catch (NullPointerException npe){
                    /*Somehow nulls are being returned even after filter*/
                    return;
                }

            }else{
                tweetFeatureVector = tweetFeatureVectorGenerator.getFeatureVector(tweet, type);
            }

            context.write(new LongWritable(tweet.getId()),
                            new Text(tweetFeatureVector));
        }
    }


    @Override
    public KijiDataRequest getDataRequest() {
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
        return LongWritable.class;
    }


}
