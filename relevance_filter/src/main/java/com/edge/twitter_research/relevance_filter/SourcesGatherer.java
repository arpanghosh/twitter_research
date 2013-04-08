package com.edge.twitter_research.relevance_filter;


import com.edge.twitter_research.core.SimpleTweet;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueWriter;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

import org.jsoup.Jsoup;

import java.io.IOException;


public class SourcesGatherer
    extends KijiGatherer<AvroKey<CharSequence>, AvroValue<Long>>
    implements AvroKeyWriter, AvroValueWriter {

    private AvroValue<Long> ONE;

    @Override
    public void setup(GathererContext<AvroKey<CharSequence>, AvroValue<Long>> context) throws IOException {
        super.setup(context); // Any time you override setup, call super.setup(context);
        ONE = new AvroValue<Long>(1L);
    }


    @Override
    public void gather(KijiRowData input, GathererContext<AvroKey<CharSequence>, AvroValue<Long>> context)
            throws IOException {

        SimpleTweet simpleTweet =
                input.getMostRecentValue(Constants.TWEET_COLUMN_FAMILY_NAME, Constants.TWEET_OBJECT_COLUMN_NAME);

        context.write(new AvroKey<CharSequence>(Jsoup.parse(simpleTweet.getSource().toString()).text()), ONE);
    }


    @Override
    public KijiDataRequest getDataRequest() {
        final KijiDataRequestBuilder builder = KijiDataRequest.builder();
        builder.newColumnsDef()
                .add(Constants.TWEET_COLUMN_FAMILY_NAME, Constants.TWEET_OBJECT_COLUMN_NAME);
        return builder.build();
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
        return Schema.create(Schema.Type.STRING);
    }


    @Override
    public Schema getAvroValueWriterSchema() throws IOException {
        return Schema.create(Schema.Type.LONG);
    }



}
