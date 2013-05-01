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
import org.apache.hadoop.io.Text;
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


public class SpecificDateGatherer
        extends KijiGatherer<Text, LongWritable>{

    private Calendar calendar;
    private SimpleDateFormat twitterDateFormat;
    private int dayOfMonth;
    private int monthOfYear;
    private StopWords stopWords;
    private LongWritable ONE;



    @Override
    public void setup(GathererContext<Text, LongWritable> context)
            throws IOException {
        super.setup(context); // Any time you override setup, call super.setup(context);

        calendar = Calendar.getInstance();
        twitterDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");

        Configuration configuration = getConf();
        dayOfMonth = configuration.getInt("day.of.month", 11);
        monthOfYear = configuration.getInt("month.of.year", 3);

        stopWords = new StopWords();

        ONE = new LongWritable(1L);

    }


    @Override
    public void gather(KijiRowData input, GathererContext<Text, LongWritable> context)
            throws IOException {


        NavigableMap<Long, CompanyData> companies =
                input.getValues(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.COMPANY_DATA_COLUMN_NAME);

        for (CompanyData companyData : companies.values()){
            if (companyData.getCompanyName().toString().equals("toyota")){

                SimpleTweet simpleTweet = input.getMostRecentValue(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.TWEET_COLUMN_NAME);

                Date tweetDate;
                try{
                    tweetDate = twitterDateFormat.parse(simpleTweet.getCreatedAt().toString());
                    calendar.setTime(tweetDate);
                }catch(ParseException parseException){
                    return;
                }catch (NullPointerException nullPointerException){
                    return;
                }

                if ((calendar.get(Calendar.MONTH) == monthOfYear) &&
                        (calendar.get(Calendar.DAY_OF_MONTH) == dayOfMonth)){
                    String[] tokens = simpleTweet.getText().toString().toLowerCase().split(" ");

                    for (String token : tokens){

                        if (!stopWords.isStopWord(token)){
                            context.write(new Text(token), ONE);
                        }

                    }

                }


                break;
            }
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
        return LongWritable.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return Text.class;
    }




}