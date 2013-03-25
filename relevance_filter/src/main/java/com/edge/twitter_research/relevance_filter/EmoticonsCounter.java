package com.edge.twitter_research.relevance_filter;


import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueReader;
import org.kiji.mapreduce.avro.AvroValueWriter;

import java.io.IOException;

public class EmoticonsCounter
        extends KijiReducer<AvroKey<CharSequence>, AvroValue<Long>, AvroKey<CharSequence>, AvroValue<Long>>
        implements AvroKeyReader, AvroKeyWriter, AvroValueReader, AvroValueWriter {

    @Override
    public Class<?> getOutputKeyClass() {
        return AvroKey.class;
    }


    @Override
    public Class<?> getOutputValueClass() {
        return AvroValue.class;
    }


    @Override
    public Schema getAvroKeyReaderSchema() throws IOException {
        return Schema.create(Schema.Type.STRING);
    }


    @Override
    public Schema getAvroKeyWriterSchema() throws IOException {
        return Schema.create(Schema.Type.STRING);
    }


    @Override
    public Schema getAvroValueReaderSchema() throws IOException {
        return Schema.create(Schema.Type.LONG);
    }


    @Override
    public Schema getAvroValueWriterSchema() throws IOException {
        return Schema.create(Schema.Type.LONG);
    }


    @Override
    protected void reduce(AvroKey<CharSequence> key, Iterable<AvroValue<Long>> values, Context context)
            throws IOException, InterruptedException {

        long sum = 0;

        for (AvroValue<Long> value : values)
            sum += value.datum();

        // output sum
        context.write(key, new AvroValue<Long>(sum));
    }
}
