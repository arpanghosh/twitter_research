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
import java.util.Comparator;
import java.util.TreeSet;



public class WordCounter
        extends KijiReducer<Text, LongWritable,
        Text, LongWritable>{


    @Override
    public Class<?> getOutputKeyClass() {
        return Text.class;
    }


    @Override
    public Class<?> getOutputValueClass() {
        return LongWritable.class;
    }



    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        long i = 0;
        for (LongWritable count : values)
            i++;

        context.write(key, new LongWritable(i));

    }

}
