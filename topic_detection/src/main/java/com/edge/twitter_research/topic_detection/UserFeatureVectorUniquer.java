package com.edge.twitter_research.topic_detection;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiReducer;


import java.io.IOException;

public class UserFeatureVectorUniquer
        extends KijiReducer<LongWritable, Text, LongWritable, Text>{

    @Override
    public Class<?> getOutputKeyClass() {
        return LongWritable.class;
    }


    @Override
    public Class<?> getOutputValueClass() {
        return Text.class;
    }


    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }

}
