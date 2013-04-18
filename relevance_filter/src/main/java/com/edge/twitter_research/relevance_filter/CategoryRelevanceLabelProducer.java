package com.edge.twitter_research.relevance_filter;


import com.edge.twitter_research.core.SimpleTweet;
import org.apache.avro.util.Utf8;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.RequiredStores;
import org.kiji.mapreduce.kvstore.lib.KijiTableKeyValueStore;
import org.kiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

import java.io.IOException;
import java.util.Map;

public class CategoryRelevanceLabelProducer
    extends KijiProducer {


    @Override
    public KijiDataRequest getDataRequest() {
        return KijiDataRequest.create(Constants.TWEET_COLUMN_FAMILY_NAME,
                                        Constants.TWEET_OBJECT_COLUMN_NAME);
    }


    /** {@inheritDoc} */
    @Override
    public String getOutputColumn() {
        // This is the output column of the kiji table that we write to.
        return (Constants.TWEET_COLUMN_FAMILY_NAME + ":" +
                Constants.TWEET_RELEVANCE_LABEL_COLUMN_NAME);
    }

    /** {@inheritDoc} */
    @Override
    public void produce(KijiRowData input, ProducerContext context) throws IOException {
        // Open the key value store reader.
        KeyValueStoreReader<String, Utf8> relevanceLabelReader = context.getStore("relevanceLabels");

        SimpleTweet simpleTweet = input.getMostRecentValue(Constants.TWEET_COLUMN_FAMILY_NAME,
                                                            Constants.TWEET_OBJECT_COLUMN_NAME);
        Utf8 label = relevanceLabelReader.get(simpleTweet.getId().toString());

        if (label != null)
            context.put(label);
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
        return RequiredStores.just("relevanceLabels", UnconfiguredKeyValueStore.builder().build());
    }

}
