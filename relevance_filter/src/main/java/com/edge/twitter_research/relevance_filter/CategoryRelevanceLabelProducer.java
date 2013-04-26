package com.edge.twitter_research.relevance_filter;


import com.edge.twitter_research.core.GlobalConstants;
import com.edge.twitter_research.core.KijiConnection;
import com.edge.twitter_research.core.SimpleTweet;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.kiji.mapreduce.KijiContext;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.RequiredStores;
import org.kiji.mapreduce.kvstore.lib.KijiTableKeyValueStore;
import org.kiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;

import java.io.IOException;
import java.util.Map;

public class CategoryRelevanceLabelProducer
    extends KijiProducer {

    KijiConnection kijiConnection;

    @Override
    public void setup (KijiContext kijiContext) throws IOException{
        super.setup(kijiContext);

        Configuration conf = getConf();
        String tempTableName =
                conf.get("temporary.table.name",
                        "category_relevance_label_temp_table");
        kijiConnection = new KijiConnection(tempTableName);
    }


    @Override
    public KijiDataRequest getDataRequest() {
        return KijiDataRequest.create(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                                        GlobalConstants.TWEET_COLUMN_NAME);
    }


    /** {@inheritDoc} */
    @Override
    public String getOutputColumn() {
        // This is the output column of the kiji table that we write to.
        return (GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME + ":" +
                GlobalConstants.RELEVANCE_LABEL_COLUMN_NAME);
    }

    /** {@inheritDoc} */
    @Override
    public void produce(KijiRowData input, ProducerContext context) throws IOException {
        // Open the key value store reader.
        KeyValueStoreReader<EntityId, Utf8> relevanceLabelReader = context.getStore("relevanceLabels");

        SimpleTweet simpleTweet = input.getMostRecentValue(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                                                            GlobalConstants.TWEET_COLUMN_NAME);
        Utf8 label =
                relevanceLabelReader.get(kijiConnection
                                        .kijiTable.getEntityId(simpleTweet.getId().toString()));

        if (label != null)
            context.put(label);
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
        return RequiredStores.just("relevanceLabels", UnconfiguredKeyValueStore.builder().build());
    }

}
