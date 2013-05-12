package com.edge.twitter_research.queries;


import com.edge.twitter_research.core.CompanyData;
import com.edge.twitter_research.core.GlobalConstants;
import com.edge.twitter_research.core.SimpleTweet;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.hbase.HConstants;
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


public class TokenizeCompanyTweetsPerTimeGatherer
        extends KijiGatherer<AvroKey<CompanyGranularDate>, AvroValue<WordCount>>
        implements AvroKeyWriter, AvroValueWriter {

    private Calendar calendar;
    private SimpleDateFormat twitterDateFormat;
    private GranularDate granularDate;
    private CompanyGranularDate companyGranularDate;
    private WordCount wordCount;

    private StopWords stopWords;


    @Override
    public void setup(GathererContext<AvroKey<CompanyGranularDate>, AvroValue<WordCount>> context)
            throws IOException {
        super.setup(context); // Any time you override setup, call super.setup(context);

        calendar = Calendar.getInstance();
        twitterDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
        granularDate = new GranularDate();
        companyGranularDate = new CompanyGranularDate();
        wordCount = new WordCount();

        stopWords = new StopWords();

        granularDate.setHourOfDay(-1);
        granularDate.setYear(-1);
        granularDate.setMonthOfYear(-1);
        granularDate.setWeekOfYear(-1);
        granularDate.setDayOfWeek(-1);
        granularDate.setDayOfMonth(-1);

    }


    @Override
    public void gather(KijiRowData input, GathererContext<AvroKey<CompanyGranularDate>, AvroValue<WordCount>> context)
            throws IOException {

        NavigableMap<Long, CompanyData> companies =
                input.getValues(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                        GlobalConstants.COMPANY_DATA_COLUMN_NAME);

        if (companies.size() > 0){

            SimpleTweet tweet = input.getMostRecentValue(GlobalConstants.TWEET_OBJECT_COLUMN_FAMILY_NAME,
                    GlobalConstants.TWEET_COLUMN_NAME);

            try{
                Date tweetDate = twitterDateFormat.parse(tweet.getCreatedAt().toString());
                calendar.setTime(tweetDate);
            }catch(ParseException parseException){
                return;
            }catch (NullPointerException nullPointerException){
                return;
            }


            granularDate.setDayOfYear(calendar.get(Calendar.DAY_OF_YEAR));


            for (CompanyData companyData : companies.values()){

                String[] tokens = tweet.getText().toString().toLowerCase().split(" ");

                for (String token : tokens){


                    wordCount.setWord(token);
                    wordCount.setCount(1L);

                    companyGranularDate.setDate(granularDate);
                    companyGranularDate.setCompanyName(companyData.getCompanyName());

                    context.write(new AvroKey<CompanyGranularDate>(companyGranularDate),
                            new AvroValue<WordCount>(wordCount));

                }
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
        return AvroValue.class;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return AvroKey.class;
    }

    @Override
    public Schema getAvroKeyWriterSchema() throws IOException {
        return CompanyGranularDate.SCHEMA$;
    }

    @Override
    public Schema getAvroValueWriterSchema() throws IOException {
        return WordCount.SCHEMA$;
    }


}