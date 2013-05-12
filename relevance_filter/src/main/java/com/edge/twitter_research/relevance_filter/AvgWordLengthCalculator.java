package com.edge.twitter_research.relevance_filter;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroValueReader;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;



public class AvgWordLengthCalculator
        extends KijiReducer<Text, Text, Text, DoubleWritable>{



    @Override
    public Class<?> getOutputValueClass() {
        return DoubleWritable.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return Text.class;
    }







    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double totalChars = 0;
        int numWords = 0;

        for (Text value : values) {
            totalChars += value.toString().length();
            numWords++;
        }

        context.write(key, new DoubleWritable(totalChars/numWords));

    }

}

