package com.edge.twitter_research.queries;



import com.edge.twitter_research.core.CompanyData;
import com.edge.twitter_research.core.GlobalConstants;
import com.edge.twitter_research.core.SimpleTweet;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

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
import java.util.*;

public class TimeCompanyGatherer
        extends KijiGatherer<AvroKey<GranularDate>, AvroValue<CompanyOccurrenceType>>
        implements AvroKeyWriter, AvroValueWriter{

    private Calendar calendar;
    private SimpleDateFormat twitterDateFormat;
    private GranularDate granularDate;
    private CompanyOccurrenceType companyOccurrenceType;


    @Override
    public void setup(GathererContext<AvroKey<GranularDate>, AvroValue<CompanyOccurrenceType>> context)
            throws IOException {
        super.setup(context); // Any time you override setup, call super.setup(context);

        calendar = Calendar.getInstance();
        twitterDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
        granularDate = new GranularDate();
        companyOccurrenceType = new CompanyOccurrenceType();

    }


    @Override
    public void gather(KijiRowData input, GathererContext<AvroKey<GranularDate>, AvroValue<CompanyOccurrenceType>> context)
            throws IOException {

        SimpleTweet tweet =
                input.getMostRecentValue(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                                         GlobalConstants.TWEET_COLUMN_NAME);
        try{
            Date tweetDate = twitterDateFormat.parse(tweet.getCreatedAt().toString());
            calendar.setTime(tweetDate);
        }catch(ParseException parseException){
            return;
        }catch (NullPointerException nullPointerException){
            return;
        }

        granularDate.setYear(calendar.get(Calendar.YEAR));
        granularDate.setMonthOfYear(calendar.get(Calendar.MONTH));
        granularDate.setWeekOfMonth(calendar.get(Calendar.WEEK_OF_MONTH));
        granularDate.setDayOfMonth(calendar.get(Calendar.DAY_OF_MONTH));
        granularDate.setHourOfDay(calendar.get(Calendar.HOUR_OF_DAY));

        NavigableMap<Long, CompanyData> companies =
            input.getValues(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                            GlobalConstants.COMPANY_DATA_COLUMN_NAME);

        for (CompanyData companyData : companies.values()){
            companyOccurrenceType.setCompanyName(companyData.getCompanyName());
            companyOccurrenceType.setCompanyArea(companyData.getCompanyArea());
            companyOccurrenceType.setIsRetweet(tweet.getIsRetweet());

            context.write(new AvroKey<GranularDate>(granularDate),
                            new AvroValue<CompanyOccurrenceType>(companyOccurrenceType));
        }
    }



    @Override
    public KijiDataRequest getDataRequest() {
        final KijiDataRequestBuilder builder = KijiDataRequest.builder();
        builder.newColumnsDef()
                .withMaxVersions(HConstants.ALL_VERSIONS)
                .addFamily(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME);
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
        return GranularDate.SCHEMA$;
    }

    @Override
    public Schema getAvroValueWriterSchema() throws IOException {
        return CompanyOccurrenceType.SCHEMA$;
    }


}

