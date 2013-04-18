package com.edge.twitter_research.relevance_filter;


import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.KijiTableReducer;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroValueReader;
import org.kiji.mapreduce.framework.HFileKeyValue;

import java.io.IOException;

public class RelevanceLabelWriter
        extends KijiTableReducer<AvroKey<Long>, AvroValue<CharSequence>>
        implements AvroKeyReader, AvroValueReader{

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context); // Any time you override setup, call super.setup(context);
    }

    @Override
    protected void reduce(AvroKey<Long> key, Iterable<AvroValue<CharSequence>> values,
                          KijiTableContext context) throws IOException {

        for (AvroValue<CharSequence> label : values){
            context.put(context.getEntityId(key.datum()),
                                        Constants.TWEET_COLUMN_FAMILY_NAME,
                                        Constants.TWEET_RELEVANCE_LABEL_COLUMN_NAME,
                                        label.datum());
        }
    }



    @Override
    public Schema getAvroKeyReaderSchema() throws IOException {
        return Schema.create(Schema.Type.LONG);
    }


    @Override
    public Schema getAvroValueReaderSchema() throws IOException {
        return Schema.create(Schema.Type.STRING);
    }

}
