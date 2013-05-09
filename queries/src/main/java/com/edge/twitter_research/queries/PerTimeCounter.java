package com.edge.twitter_research.queries;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.avro.AvroKeyReader;


import java.io.IOException;


public class PerTimeCounter
        extends KijiReducer<AvroKey<GranularDate>, LongWritable,
        Text, LongWritable>
        implements AvroKeyReader {

    private String granularity;

    public Schema getAvroKeyReaderSchema() throws IOException {
        return GranularDate.SCHEMA$;
    }


    public void setup(Context context)throws IOException, InterruptedException{
        super.setup(context);

        Configuration configuration = context.getConfiguration();
        granularity = configuration.get("granularity", "daily");
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return Text.class;
    }


    @Override
    public Class<?> getOutputValueClass() {
        return LongWritable.class;
    }



    @Override
    protected void reduce(AvroKey<GranularDate> key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        long count = 0;

        for (LongWritable value : values){
            count++;
        }

        if (granularity.equals("weekly")){
            context.write(new Text(GranularDate.newBuilder(key.datum()).build().getWeekOfYear().toString()),
                    new LongWritable(count));
        }else if (granularity.equals("daily")){
            context.write(new Text(GranularDate.newBuilder(key.datum()).build().getDayOfYear().toString()),
                    new LongWritable(count));
        }else{
            GranularDate gd = GranularDate.newBuilder(key.datum()).build();
            String dateString = gd.getDayOfYear() + ":" + gd.getHourOfDay();

            context.write(new Text(dateString),
                    new LongWritable(count));
        }
    }
}