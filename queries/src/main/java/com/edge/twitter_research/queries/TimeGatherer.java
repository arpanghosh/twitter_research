package com.edge.twitter_research.queries;

import com.edge.twitter_research.core.CompanyData;
import com.edge.twitter_research.core.GlobalConstants;
import com.edge.twitter_research.core.SimpleTweet;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
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
import java.util.Calendar;
import java.util.Date;
import java.util.NavigableMap;


public class TimeGatherer
        extends KijiGatherer<AvroKey<GranularDate>, LongWritable>
        implements AvroKeyWriter {

    private Calendar calendar;
    private SimpleDateFormat twitterDateFormat;
    private GranularDate granularDate;
    private final LongWritable ONE = new LongWritable(1L);
    private String granularity;


    @Override
    public void setup(GathererContext<AvroKey<GranularDate>, LongWritable> context)
            throws IOException {
        super.setup(context); // Any time you override setup, call super.setup(context);

        calendar = Calendar.getInstance();
        twitterDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
        granularDate = new GranularDate();

        Configuration configuration = getConf();
        granularity = configuration.get("granularity", "daily");

        if (granularity.equals("weekly")){
            granularDate.setDayOfYear(-1);
            granularDate.setDayOfMonth(-1);
            granularDate.setDayOfWeek(-1);
            granularDate.setHourOfDay(-1);
        }else if (granularity.equals("daily")){
            granularDate.setHourOfDay(-1);
        }

    }


    @Override
    public void gather(KijiRowData input, GathererContext<AvroKey<GranularDate>, LongWritable> context)
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
        granularDate.setWeekOfYear(calendar.get(Calendar.WEEK_OF_YEAR));

        if (granularity.equals("daily")){
            granularDate.setDayOfYear(calendar.get(Calendar.DAY_OF_YEAR));
            granularDate.setDayOfMonth(calendar.get(Calendar.DAY_OF_MONTH));
            granularDate.setDayOfWeek(calendar.get(Calendar.DAY_OF_WEEK));
        } else if (granularity.equals("hourly")){
            granularDate.setDayOfYear(calendar.get(Calendar.DAY_OF_YEAR));
            granularDate.setDayOfMonth(calendar.get(Calendar.DAY_OF_MONTH));
            granularDate.setDayOfWeek(calendar.get(Calendar.DAY_OF_WEEK));
            granularDate.setHourOfDay(calendar.get(Calendar.HOUR_OF_DAY));
        }

        context.write(new AvroKey<GranularDate>(granularDate),
                ONE);

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
        return LongWritable.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return AvroKey.class;
    }

    @Override
    public Schema getAvroKeyWriterSchema() throws IOException {
        return GranularDate.SCHEMA$;
    }



}
