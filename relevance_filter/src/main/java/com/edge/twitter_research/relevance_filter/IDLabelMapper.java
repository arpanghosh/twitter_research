package com.edge.twitter_research.relevance_filter;


import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueWriter;

import java.io.IOException;

public class IDLabelMapper
        extends KijiMapper<LongWritable, Text, AvroKey<Long>, AvroValue<CharSequence>>
        implements AvroKeyWriter, AvroValueWriter {


    /** {@inheritDoc} */
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");

        context.write(new AvroKey<Long>(Long.parseLong(tokens[1].replaceAll("^\"|\"$", ""))),
                        new AvroValue<CharSequence>(tokens[4].replaceAll("^\"|\"$", "")));
    }

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
        return Schema.create(Schema.Type.LONG);
    }


    @Override
    public Schema getAvroValueWriterSchema() throws IOException {
        return Schema.create(Schema.Type.STRING);
    }

}
