package com.edge.twitter_research.queries;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiMapper;

import java.io.IOException;


public class WikiCategoriesCleanerSampler
        extends KijiMapper<LongWritable, Text,
        Text, Text> {

    private double threshold;

    @Override
    public Class<?> getOutputValueClass() {
        return Text.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return Text.class;
    }



    @Override
    public void setup(Context context)
            throws IOException, InterruptedException{
        super.setup(context);

        Configuration conf = context.getConfiguration();
        threshold = conf.getFloat("sampling.rate", 100)/100.0;
    }


    @Override
    public void map(LongWritable rowNumber, Text row, Context context)
            throws IOException, InterruptedException {

        if (Math.random() < threshold){

            String[] tokens = row.toString().split(" ");
            String article = tokens[0].substring(30, tokens[0].length() - 1);
            String to = tokens[2].substring(39, tokens[2].length() - 1);

            context.write(new Text(article), new Text(to));
        }
    }
}
