package com.edge.twitter_research.queries;

import com.edge.twitter_research.core.GlobalConstants;
import com.edge.twitter_research.core.SimpleTweet;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueWriter;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

import java.io.IOException;

public class CategoryUsersGatherer
        extends KijiGatherer<AvroKey<Long>, AvroValue<SimpleTweet>>
        implements AvroKeyWriter, AvroValueWriter {


    @Override
    public void gather(KijiRowData input, GathererContext<AvroKey<Long>, AvroValue<SimpleTweet>> context)
            throws IOException {
            SimpleTweet tweet = input.getMostRecentValue(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                    GlobalConstants.TWEET_COLUMN_NAME);

            context.write(new AvroKey<Long>(tweet.getUser().getId()),
                    new AvroValue<SimpleTweet>(tweet));
    }



    @Override
    public KijiDataRequest getDataRequest() {
        final KijiDataRequestBuilder builder = KijiDataRequest.builder();
        builder.newColumnsDef()
                .withMaxVersions(1)
                .add(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME, GlobalConstants.TWEET_COLUMN_NAME);
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
        return Schema.create(Schema.Type.LONG);
    }

    @Override
    public Schema getAvroValueWriterSchema() throws IOException {
        return SimpleTweet.SCHEMA$;
    }


}