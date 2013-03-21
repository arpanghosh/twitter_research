package com.edge.twitter_research.relevance_filter;


import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.hbase.HConstants;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueWriter;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

import java.io.IOException;

public class TopEmoticonsGatherer
    extends KijiGatherer<AvroKey<CharSequence>, AvroValue<EmoticonCount>>
    implements AvroValueWriter, AvroKeyWriter{

    private EmoticonCount mEmoticonCount;
    private CharSequence emoticon = "emoticon";

    @Override
    public void setup(GathererContext<AvroKey<CharSequence>, AvroValue<EmoticonCount>> context) throws IOException {
        super.setup(context); // Any time you override setup, call super.setup(context);
        mEmoticonCount = new EmoticonCount();
    }


    @Override
    public void gather(KijiRowData input, GathererContext<AvroKey<CharSequence>, AvroValue<EmoticonCount>> context)
            throws IOException {

        mEmoticonCount.setEmoticon(input.getEntityId().getComponentByIndex(0).toString());
        mEmoticonCount.setCount((long)input.getValues("emoticon_occurrence", "tweet_id").size());

        context.write(new AvroKey<CharSequence>(emoticon),
                new AvroValue<EmoticonCount>(mEmoticonCount));
    }


    @Override
    public KijiDataRequest getDataRequest() {
        // This method is how we specify which columns in each row the gatherer operates on.
        // In this case, we need all versions of the emoticon_occurrence:tweet_id column.
        final KijiDataRequestBuilder builder = KijiDataRequest.builder();
        builder.newColumnsDef()
                .withMaxVersions(HConstants.ALL_VERSIONS) // Retrieve all versions.
                .withPageSize(10)
                .add("emoticon_occurrence", "tweet_id");
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
    public Schema getAvroValueWriterSchema() throws IOException {
        // Since we are writing AvroValues, we need to specify the schema.
        return EmoticonCount.SCHEMA$;
    }

    @Override
    public Schema getAvroKeyWriterSchema() throws IOException {
        // Since we are writing AvroValues, we need to specify the schema.
        return Schema.create(Schema.Type.STRING);
    }


}