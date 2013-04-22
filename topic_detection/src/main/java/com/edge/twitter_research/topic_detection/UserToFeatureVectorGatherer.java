package com.edge.twitter_research.topic_detection;


import com.edge.twitter_research.core.CompanyData;
import com.edge.twitter_research.core.GlobalConstants;
import com.edge.twitter_research.core.SimpleTweet;
import com.edge.twitter_research.core.SimpleUser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

import java.io.IOException;
import java.util.NavigableMap;

public class UserToFeatureVectorGatherer
        extends KijiGatherer<LongWritable, Text> {

    private String companyName;
    private UserFeatureVectorGenerator userFeatureVectorGenerator;

    @Override
    public void setup(GathererContext<LongWritable, Text> context) throws IOException {
        super.setup(context); // Any time you override setup, call super.setup(context);

        userFeatureVectorGenerator = new UserFeatureVectorGenerator();

        Configuration conf = getConf();
        companyName = conf.get("company.name", "");
    }


    @Override
    public void gather(KijiRowData input, GathererContext<LongWritable, Text> context)
            throws IOException {

        NavigableMap<Long, CompanyData> companies =
                input.getValues(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                                GlobalConstants.COMPANY_DATA_COLUMN_NAME);

        String featureVector;

        for (CompanyData company : companies.values()){
            if (company.getCompanyName().toString().equals(companyName)){
                SimpleTweet simpleTweet =
                        input.getMostRecentValue(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                                                GlobalConstants.TWEET_COLUMN_NAME);
                featureVector = userFeatureVectorGenerator.getFeatureVector(simpleTweet.getUser());
                context.write(new LongWritable(simpleTweet.getUser().getId()),
                                new Text(featureVector));
                break;
            }
        }
    }



    @Override
    public KijiDataRequest getDataRequest() {
        // This method is how we specify which columns in each row the gatherer operates on.
        // In this case, we need all versions of the tweet_object:tweet column.
        final KijiDataRequestBuilder builder = KijiDataRequest.builder();
        builder.newColumnsDef()
                .withMaxVersions(HConstants.ALL_VERSIONS)
                .addFamily(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME);
        return builder.build();
    }


    @Override
    public Class<?> getOutputValueClass() {
        return Text.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return LongWritable.class;
    }
}
