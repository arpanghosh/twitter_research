package com.edge.twitter_research.relevance_filter;


import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueWriter;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiPager;
import org.kiji.schema.KijiRowData;

import java.io.IOException;

public class EmoticonsMapper
    extends KijiMapper<LongWritable, Text, AvroKey<CharSequence>, AvroValue<Long>>
    implements AvroValueWriter, AvroKeyWriter{

    private final AvroValue<Long> ONE =
            new AvroValue<Long>(1L);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] tokens = value.toString().split("\\t");

        context.write(new AvroKey<CharSequence>(tokens[1]), ONE);
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
    public Schema getAvroValueWriterSchema() throws IOException {
        // Since we are writing AvroValues, we need to specify the schema.
        return Schema.create(Schema.Type.LONG);
    }

    @Override
    public Schema getAvroKeyWriterSchema() throws IOException {
        // Since we are writing AvroValues, we need to specify the schema.
        return Schema.create(Schema.Type.STRING);
    }


}
