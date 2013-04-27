package com.edge.twitter_research.topic_detection;


import com.edge.twitter_research.core.CompanyData;
import com.edge.twitter_research.core.GlobalConstants;
import com.edge.twitter_research.core.SimpleTweet;
import com.edge.twitter_research.core.SimpleUser;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.LongWritable;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueWriter;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.NavigableMap;

public class UserCompanyGatherer
        extends KijiGatherer<AvroKey<UserCompany>, AvroValue<UserVersion>>
        implements AvroValueWriter, AvroKeyWriter {

    UserCompany userCompany;
    UserVersion userVersion;
    User user;

    SimpleDateFormat twitterDateFormat;

    @Override
    public void setup(GathererContext<AvroKey<UserCompany>, AvroValue<UserVersion>> context) throws IOException {
        super.setup(context); // Any time you override setup, call super.setup(context);
        userCompany = new UserCompany();
        user = new User();
        userVersion = new UserVersion();

        twitterDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
    }


    @Override
    public void gather(KijiRowData input, GathererContext<AvroKey<UserCompany>, AvroValue<UserVersion>> context)
            throws IOException {


        NavigableMap<Long, CompanyData> companies =
                input.getValues(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.COMPANY_DATA_COLUMN_NAME);

        SimpleTweet tweet = input.getMostRecentValue(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                GlobalConstants.TWEET_COLUMN_NAME);

        for (CompanyData company : companies.values()){

            userCompany.setUserId(tweet.getUser().getId());
            userCompany.setCompanyName(company.getCompanyName());
            userCompany.setCompanyArea(company.getCompanyArea());

            user.setCreatedAt(tweet.getUser().getCreatedAt());
            user.setIsVerified(tweet.getUser().getVerified());
            user.setNumFollowers(tweet.getUser().getFollowersCount());
            user.setNumFriends(tweet.getUser().getFriendsCount());
            user.setNumStatuses(tweet.getUser().getStatusesCount());
            user.setNumListed(tweet.getUser().getListedCount());
            user.setScreenName(tweet.getUser().getScreenName());
            user.setUserId(tweet.getUser().getId());

            userVersion.setUser(user);
            userVersion.setVersion(tweet.getId());

            context.write(new AvroKey<UserCompany>(userCompany),
                    new AvroValue<UserVersion>(userVersion));
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
    public Schema getAvroKeyWriterSchema() throws IOException {
        return UserCompany.SCHEMA$;
    }


    @Override
    public Schema getAvroValueWriterSchema() throws IOException {
        return UserVersion.SCHEMA$;
    }

    @Override
    public Class<?> getOutputValueClass() {
        return AvroValue.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return AvroKey.class;
    }
}
