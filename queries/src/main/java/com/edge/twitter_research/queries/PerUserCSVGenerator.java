package com.edge.twitter_research.queries;


import com.edge.twitter_research.core.SimpleTweet;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroValueReader;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

public class PerUserCSVGenerator
        extends KijiReducer<AvroKey<Long>, AvroValue<SimpleTweet>,
        LongWritable, Text>
        implements AvroValueReader, AvroKeyReader {

    private TreeSet<SimpleTweet> categoryUserTweets;
    private DecimalFormat fractionFormat;

    public Schema getAvroKeyReaderSchema() throws IOException {
        return Schema.create(Schema.Type.LONG);
    }

    public Schema getAvroValueReaderSchema() throws IOException {
        return SimpleTweet.SCHEMA$;
    }


    public void setup(Context context)throws IOException, InterruptedException{
        super.setup(context);

        categoryUserTweets = new TreeSet<SimpleTweet>(new Comparator<SimpleTweet>() {
            @Override
            public int compare(SimpleTweet simpleTweet1, SimpleTweet simpleTweet2) {
                return simpleTweet1.getRetweetCount().compareTo(simpleTweet2.getRetweetCount());
            }
        });

        fractionFormat = new DecimalFormat("0.00000");

    }

    @Override
    public Class<?> getOutputKeyClass() {
        return LongWritable.class;
    }


    @Override
    public Class<?> getOutputValueClass() {
        return Text.class;
    }



    @Override
    protected void reduce(AvroKey<Long> key, Iterable<AvroValue<SimpleTweet>> values, Context context)
            throws IOException, InterruptedException {


        categoryUserTweets.clear();


        for (AvroValue<SimpleTweet> value : values){
            SimpleTweet simpleTweet =
                    SimpleTweet.newBuilder(value.datum()).build();

            categoryUserTweets.add(simpleTweet);
        }

        double averageRetweets = 0.0;
        int numOriginals = 0;
        int numRetweets = 0;
        long maxRetweets;

        for (SimpleTweet simpleTweet : categoryUserTweets){
            averageRetweets += simpleTweet.getRetweetCount();

            if (simpleTweet.getIsRetweet())
                numRetweets++;
            else
                numOriginals++;
        }

        averageRetweets = averageRetweets/categoryUserTweets.size();
        maxRetweets = categoryUserTweets.last().getRetweetCount();


        SimpleTweet representative = categoryUserTweets.first();
        StringBuilder features = new StringBuilder();
        features.append(representative.getUser().getScreenName());
        features.append(",");
        features.append(fractionFormat.format(averageRetweets));
        features.append(",");
        features.append(maxRetweets);
        features.append(",");
        features.append(numOriginals);
        features.append(",");
        features.append(numRetweets);
        features.append(",");
        features.append(representative.getUser().getStatusesCount());
        features.append(",");
        features.append(representative.getUser().getFollowersCount());
        features.append(",");
        features.append(representative.getUser().getFriendsCount());
        features.append(",");
        features.append(representative.getUser().getListedCount());
        features.append(",");
        features.append(representative.getUser().getVerified());

        context.write(new LongWritable(key.datum()), new Text(features.toString()));
    }
}
