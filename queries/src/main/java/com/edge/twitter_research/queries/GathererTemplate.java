package com.edge.twitter_research.queries;



import com.edge.twitter_research.core.CompanyData;
import com.edge.twitter_research.core.GlobalConstants;
import com.edge.twitter_research.core.SimpleTweet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class GathererTemplate
        extends KijiGatherer<LongWritable, Text> {

    private Calendar c;
    private SimpleDateFormat twitterDateFormat;

    @Override
    public void setup(GathererContext<LongWritable, Text> context) throws IOException {
        super.setup(context); // Any time you override setup, call super.setup(context);
        Calendar c = Calendar.getInstance();
        SimpleDateFormat twitterDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
    }


    @Override
    public void gather(KijiRowData input, GathererContext<LongWritable, Text> context)
            throws IOException {

        SimpleTweet tweet = input.getMostRecentValue(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                GlobalConstants.TWEET_COLUMN_NAME);

        Date tweetDate = null;

        try{
            tweetDate = twitterDateFormat.parse(tweet.getCreatedAt().toString());
        }catch(ParseException parseException){
            parseException.printStackTrace();
        }catch (NullPointerException nullPointerException){
            nullPointerException.printStackTrace();
        }
        int atDate = -1;
        if (tweetDate != null){
            c.setTime(tweetDate);
            atDate = c.get(Calendar.DAY_OF_YEAR);}
        NavigableMap<Long, CompanyData> companies =
                input.getValues(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.COMPANY_DATA_COLUMN_NAME);
        for(CompanyData company : companies.values())
        context.write(new LongWritable(atDate), new Text(company.getCompanyName().toString()));
    }



    @Override
    public KijiDataRequest getDataRequest() {
        final KijiDataRequestBuilder builder = KijiDataRequest.builder();
        builder.newColumnsDef()
                .withMaxVersions(1)
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

