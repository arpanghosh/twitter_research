package com.edge.twitter_research.relevance_filter;


import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueReader;
import org.kiji.mapreduce.avro.AvroValueWriter;

import java.io.IOException;

public class CountedEmoticonsMapper
        extends KijiMapper<AvroKey<CharSequence>, AvroValue<Long>, AvroKey<CharSequence>, AvroValue<EmoticonCount>>
        implements AvroKeyReader, AvroKeyWriter, AvroValueWriter, AvroValueReader {

    private AvroKey<CharSequence> singleKey =
            new AvroKey<CharSequence>("emoticon");

    @Override
    public Class<?> getOutputValueClass() {
        return AvroValue.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return AvroKey.class;
    }

    @Override
    public Schema getAvroKeyWriterSchema() throws IOException {
        return Schema.create(Schema.Type.STRING);
    }

    @Override
    public Schema getAvroValueWriterSchema() throws IOException {
        return EmoticonCount.SCHEMA$;
    }

    @Override
    public Schema getAvroKeyReaderSchema() throws IOException {
        return Schema.create(Schema.Type.STRING);
    }

    @Override
    public Schema getAvroValueReaderSchema() throws IOException {
        return Schema.create(Schema.Type.LONG);
    }

    @Override
    public void map(AvroKey<CharSequence> key, AvroValue<Long> value, Context context)
            throws IOException, InterruptedException {
        context.write(singleKey, new AvroValue<EmoticonCount>(new EmoticonCount(key.datum(), value.datum())));
    }
}
